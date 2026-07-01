#!/usr/bin/env python3
"""
Great Lakes freighter collector.

Opens the aisstream.io websocket, listens over a Great Lakes bounding box for a
fixed window, merges live positions with a persistent static-data cache, and
writes great-lakes-vessels.json for the DakBoard display to fetch.

Why the cache: AIS position reports arrive every few seconds, but the static
data that carries a ship's NAME, TYPE, DIMENSIONS and DESTINATION only broadcasts
every few minutes. A single short listen window rarely catches static data for
every vessel, so we accumulate it across runs in vessel-static-cache.json and
join it onto whatever positions we see this run.
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

# ---- tuning ----------------------------------------------------------------
# One box covering Superior, Michigan, Huron, Erie, Ontario and the connecting
# rivers. Corner order does not matter. Split into per-lake boxes if you want to
# exclude the St. Lawrence Seaway / Erie Canal edges (the API allows several
# boxes with no duplicate data).
BBOX            = [[[41.0, -92.5], [49.2, -75.8]]]
LISTEN_SECONDS  = 55          # gather window; keep comfortably under the job timeout
MAX_VESSELS     = 40          # cap the output list; biggest ships surface first
INCLUDE_UNKNOWN = True        # keep positioned ships whose type we have not learned yet

DATA_DIR   = Path("data")
OUT_FILE   = DATA_DIR / "great-lakes-vessels.json"
CACHE_FILE = DATA_DIR / "vessel-static-cache.json"
API_KEY    = os.environ.get("AISSTREAM_API_KEY", "")

# AIS ship-type codes we treat as freighters: cargo (70-79), tanker (80-89),
# tug (52), towing (31, 32).
FREIGHTER_TYPES = set(range(70, 90)) | {52, 31, 32}


# ---- helpers ---------------------------------------------------------------
def clean(text):
    """Strip AIS '@' padding and surrounding whitespace from a text field."""
    if not text:
        return ""
    return text.replace("@", "").strip()


def parse_time(t):
    """aisstream time_utc ('2022-12-29 18:22:32.318353 +0000 UTC') -> ISO8601 Z."""
    if t:
        s = t.replace(" UTC", "").strip()
        for fmt in ("%Y-%m-%d %H:%M:%S.%f %z", "%Y-%m-%d %H:%M:%S %z"):
            try:
                return (datetime.strptime(s, fmt)
                        .astimezone(timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"))
            except ValueError:
                continue
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_freighter(vtype):
    return vtype in FREIGHTER_TYPES


def load_cache():
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text())
        except json.JSONDecodeError:
            print("cache unreadable, starting fresh", file=sys.stderr)
    return {}


# ---- collection ------------------------------------------------------------
async def collect():
    """Listen for LISTEN_SECONDS and return (positions, statics) keyed by MMSI."""
    positions, statics = {}, {}
    sub = {
        "APIKey": API_KEY,
        "BoundingBoxes": BBOX,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    async with websockets.connect(
        "wss://stream.aisstream.io/v0/stream",
        ping_interval=None, max_size=None
    ) as ws:
        await ws.send(json.dumps(sub))  # must arrive within 3s of connecting
        deadline = time.monotonic() + LISTEN_SECONDS

        while time.monotonic() < deadline:
            try:
                raw = await asyncio.wait_for(
                    ws.recv(), timeout=max(1.0, deadline - time.monotonic())
                )
            except asyncio.TimeoutError:
                break

            msg = json.loads(raw)
            if "error" in msg:
                print("aisstream error:", msg["error"], file=sys.stderr)
                break

            mtype = msg.get("MessageType")
            meta = msg.get("MetaData", {}) or {}
            mmsi = meta.get("MMSI")
            if mmsi is None:
                continue
            mmsi = str(mmsi)

            if mtype == "PositionReport":
                pr = msg["Message"]["PositionReport"]
                hdg = pr.get("TrueHeading")
                if hdg is None or hdg == 511:      # 511 = heading not available
                    hdg = round(pr.get("Cog", 0))
                positions[mmsi] = {
                    "lat": round(pr.get("Latitude", meta.get("latitude", 0)), 5),
                    "lon": round(pr.get("Longitude", meta.get("longitude", 0)), 5),
                    "sog": round(pr.get("Sog", 0), 1),
                    "cog": round(pr.get("Cog", 0)),
                    "heading": int(hdg),
                    "name_meta": clean(meta.get("ShipName", "")),
                    "lastReport": parse_time(meta.get("time_utc")),
                }

            elif mtype == "ShipStaticData":
                sd = msg["Message"]["ShipStaticData"]
                dim = sd.get("Dimension", {}) or {}
                length = (dim.get("A", 0) or 0) + (dim.get("B", 0) or 0)  # bow + stern
                beam = (dim.get("C", 0) or 0) + (dim.get("D", 0) or 0)    # port + starboard
                statics[mmsi] = {
                    "name": clean(sd.get("Name", "")),
                    "type": sd.get("Type", 0) or 0,
                    "length": length,
                    "beam": beam,
                    "destination": clean(sd.get("Destination", "")),
                    "callsign": clean(sd.get("CallSign", "")),
                }

    return positions, statics


# ---- build + write ---------------------------------------------------------
def build(positions, statics, cache):
    """Merge new static data into the cache, then join positions to produce output."""
    for mmsi, s in statics.items():
        prev = cache.get(mmsi, {})
        # never let a sparse new message wipe out good cached values
        cache[mmsi] = {
            "name":        s["name"]        or prev.get("name", ""),
            "type":        s["type"]        or prev.get("type", 0),
            "length":      s["length"]      or prev.get("length", 0),
            "beam":        s["beam"]        or prev.get("beam", 0),
            "destination": s["destination"] or prev.get("destination", ""),
            "callsign":    s["callsign"]    or prev.get("callsign", ""),
        }

    vessels = []
    for mmsi, p in positions.items():
        st = cache.get(mmsi, {})
        vtype = st.get("type", 0)
        if not is_freighter(vtype):
            # drop known non-freighters; keep unknowns only if allowed
            if vtype != 0 or not INCLUDE_UNKNOWN:
                continue
        vessels.append({
            "mmsi": int(mmsi),
            "name": st.get("name") or p["name_meta"] or f"MMSI {mmsi}",
            "type": vtype,
            "lat": p["lat"], "lon": p["lon"],
            "sog": p["sog"], "cog": p["cog"], "heading": p["heading"],
            "length": st.get("length", 0),
            "beam": st.get("beam", 0),
            "destination": st.get("destination", ""),
            "lastReport": p["lastReport"],
        })

    vessels.sort(key=lambda v: (-v["length"], v["name"]))  # biggest first
    vessels = vessels[:MAX_VESSELS]

    out = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "vessels": vessels,
    }
    return out, cache


def main():
    if not API_KEY:
        sys.exit("AISSTREAM_API_KEY environment variable is not set")

    DATA_DIR.mkdir(exist_ok=True)
    cache = load_cache()
    positions, statics = asyncio.run(collect())
    out, cache = build(positions, statics, cache)

    OUT_FILE.write_text(json.dumps(out, indent=2))
    CACHE_FILE.write_text(json.dumps(cache, indent=2))
    print(f"wrote {len(out['vessels'])} vessels; static cache holds {len(cache)}")


if __name__ == "__main__":
    main()
