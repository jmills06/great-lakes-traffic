#!/usr/bin/env python3
"""
Great Lakes freighter collector.

Opens the aisstream.io websocket, listens over a Great Lakes bounding box for a
fixed window, merges live positions with a persistent static-data cache, and
writes great-lakes-vessels.json for the DakBoard display to fetch.

A vessel is only written to the output once its cached static data confirms it
is a freighter (cargo / tanker, or a large tug) AND gives its size. That single
rule fixes two things at once: no vessel ever renders with blank details, and
harbor craft, tour boats, pilot boats and pleasure craft never reach the board.

Why the cache: AIS position reports arrive every few seconds, but the static
data carrying a ship's NAME, TYPE, DIMENSIONS and DESTINATION only broadcasts
every few minutes. A single listen window rarely catches static for every
vessel, so we accumulate it across runs in vessel-static-cache.json and join it
onto whatever positions we see this run.
"""
import asyncio
import json
import os
import ssl
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import certifi
import websockets

# ---- tuning ----------------------------------------------------------------
# One box covering Superior, Michigan, Huron, Erie, Ontario and the connecting
# rivers. Corner order does not matter. Split into per-lake boxes if you want to
# trim the St. Lawrence Seaway / Erie Canal edges (the API allows several boxes).
BBOX           = [[[41.0, -92.5], [49.2, -75.8]]]
LISTEN_SECONDS = 75            # gather window; keep well under the job timeout
MAX_VESSELS    = 40            # cap the output list; biggest ships surface first
MIN_LENGTH_M   = 75            # size floor: keeps real ships, drops harbor craft

DATA_DIR   = Path("data")
OUT_FILE   = DATA_DIR / "great-lakes-vessels.json"
CACHE_FILE = DATA_DIR / "vessel-static-cache.json"

API_KEY    = os.environ.get("AISSTREAM_API_KEY", "")

# AIS ship-type codes that count as freighters.
CARGO_TANKER = set(range(70, 90))   # 70-79 cargo, 80-89 tanker
TUG_TOWING   = {52, 31, 32}         # tug + towing (integrated tug-barge units run cargo)


def is_freighter(vtype, length):
    """A displayable freighter: cargo/tanker of any listed size, or a large tug."""
    if length < MIN_LENGTH_M:
        return False
    if vtype in CARGO_TANKER:
        return True
    if vtype in TUG_TOWING and length >= MIN_LENGTH_M:
        return True
    return False


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


def load_cache():
    if CACHE_FILE.exists():
        try:
            raw = json.loads(CACHE_FILE.read_text())
            return {str(k): v for k, v in raw.items()}   # keys always strings
        except json.JSONDecodeError:
            print("cache unreadable, starting fresh", file=sys.stderr)
    return {}


# ---- collection ------------------------------------------------------------
async def collect():
    """Listen for LISTEN_SECONDS and return (positions, statics) keyed by MMSI string."""
    positions, statics = {}, {}
    sub = {
        "APIKey": API_KEY,
        "BoundingBoxes": BBOX,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }
    # Validate the server certificate against certifi's up-to-date CA bundle
    # instead of whatever trust store the CI runner happens to ship. This is
    # what fixes the "certificate verify failed: certificate has expired"
    # handshake error when the runner's system roots are stale.
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    async with websockets.connect(
        "wss://stream.aisstream.io/v0/stream",
        ssl=ssl_ctx,
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
    """Merge new static into the cache FIRST, then join positions against the cache."""
    # 1) fold this run's static data into the persistent cache
    for mmsi, s in statics.items():
        prev = cache.get(mmsi, {})
        cache[mmsi] = {   # never let a sparse message wipe out good cached values
            "name":        s["name"]        or prev.get("name", ""),
            "type":        s["type"]        or prev.get("type", 0),
            "length":      s["length"]      or prev.get("length", 0),
            "beam":        s["beam"]        or prev.get("beam", 0),
            "destination": s["destination"] or prev.get("destination", ""),
            "callsign":    s["callsign"]    or prev.get("callsign", ""),
        }

    # 2) join positions to the (now merged) cache, keeping only real freighters
    vessels = []
    for mmsi, p in positions.items():
        st = cache.get(mmsi)
        if not st:
            continue                                   # no static yet: wait for it
        vtype = st.get("type", 0) or 0
        length = st.get("length", 0) or 0
        beam = st.get("beam", 0) or 0
        if not is_freighter(vtype, length):
            continue                                   # tour boats, tugs < floor, etc.
        vessels.append({
            "mmsi": int(mmsi),
            "name": st.get("name") or p["name_meta"] or f"MMSI {mmsi}",
            "type": vtype,
            "lat": p["lat"], "lon": p["lon"],
            "sog": p["sog"], "cog": p["cog"], "heading": p["heading"],
            "length": length,
            "beam": beam,
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
    # A transient TLS / network blip should not fail the whole run and skip the
    # commit; exit cleanly and keep the previously committed data on the board.
    # ssl.SSLCertVerificationError is a subclass of OSError, so it is covered.
    try:
        positions, statics = asyncio.run(collect())
    except (OSError, websockets.WebSocketException) as e:
        print(f"collection failed, keeping previous data: {e}", file=sys.stderr)
        sys.exit(0)
    out, cache = build(positions, statics, cache)
    OUT_FILE.write_text(json.dumps(out, indent=2))
    CACHE_FILE.write_text(json.dumps(cache, indent=2))
    print(f"wrote {len(out['vessels'])} freighters; static cache holds {len(cache)}")


if __name__ == "__main__":
    main()
