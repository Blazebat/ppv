#!/usr/bin/env python3
"""
ppv_epg.py

Full-featured XMLTV EPG generator for https://ppv.to/api/streams

Features included:
- channel id = uri_name
- icon in <channel> and <programme>
- <episode-num system="urn:stream-id"> with stream id
- strict XMLTV structure
- default timezone = Asia/Manila (configurable)
- duration in minutes
- tries to extract .m3u8 from iframe page and places <url> inside programme
- overlap detection & cleaning (trim or merge)
- outputs: epg.xml, epg.xml.gz, epg_clean.xml, epg_clean.xml.gz
- saves fallback_streams.json
- logging, CLI options
"""

from __future__ import annotations
import sys
import gzip
import json
import logging
import re
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional

# ---------- CONFIG ----------
API_URL = "https://ppv.to/api/streams"
FALLBACK_JSON = "fallback_streams.json"
EPG_XML = "epg.xml"
EPG_GZ = "epg.xml.gz"
EPG_CLEAN_XML = "epg_clean.xml"
EPG_CLEAN_GZ = "epg_clean.xml.gz"

# HTTP settings
HTTP_TIMEOUT = 20
USER_AGENT = "ppv_epg/1.0 (+https://ppv.to/)"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)

# ---------- UTIL ----------
def safe_text(v) -> str:
    return "" if v is None else str(v)

def dt_from_ts(ts: int, tz: ZoneInfo) -> datetime:
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(tz)

def xmltv_time(ts: int, tz: ZoneInfo) -> str:
    dt = dt_from_ts(ts, tz)
    off = dt.utcoffset() or timedelta(0)
    minutes = int(off.total_seconds() // 60)
    sign = '+' if minutes >= 0 else '-'
    hh = abs(minutes)//60
    mm = abs(minutes)%60
    return dt.strftime("%Y%m%d%H%M%S") + f" {sign}{hh:02d}{mm:02d}"

def write_gz(path: Path, data: bytes):
    with gzip.open(path, "wb") as f:
        f.write(data)
    logging.info(f"Wrote {path} (gzipped)")

# ---------- FETCH & FALLBACK ----------
def fetch_api(url: str) -> Dict[str, Any]:
    headers = {"User-Agent": USER_AGENT}
    try:
        logging.info(f"GET {url}")
        r = requests.get(url, timeout=HTTP_TIMEOUT, headers=headers)
        r.raise_for_status()
        data = r.json()
        Path(FALLBACK_JSON).write_text(json.dumps(data, indent=2))
        logging.info(f"Saved fallback JSON: {FALLBACK_JSON}")
        return data
    except Exception as e:
        logging.warning(f"API fetch failed: {e}")
        if Path(FALLBACK_JSON).exists():
            logging.info("Loading fallback JSON")
            return json.loads(Path(FALLBACK_JSON).read_text())
        raise

# ---------- HLS EXTRACTION ----------
M3U8_RE = re.compile(r"(https?://[^\s'\"<>]+\.m3u8[^\s'\"<>]*)", re.IGNORECASE)

def fetch_iframe_m3u8(iframe_url: str) -> Optional[str]:
    if not iframe_url:
        return None
    headers = {"User-Agent": USER_AGENT, "Referer": iframe_url}
    try:
        logging.debug(f"Fetching iframe to search for m3u8: {iframe_url}")
        r = requests.get(iframe_url, timeout=HTTP_TIMEOUT, headers=headers)
        r.raise_for_status()
        text = r.text
        # quick search for m3u8 links
        m = M3U8_RE.search(text)
        if m:
            found = m.group(1)
            logging.info(f"Found m3u8: {found}")
            return found
        # sometimes the iframe contains an embed that needs a second fetch; attempt to find an embed src
        embed_match = re.search(r'src\s*=\s*["\']([^"\']+)["\']', text)
        if embed_match:
            src = embed_match.group(1)
            if src and not src.startswith("data:") and "javascript" not in src:
                # make absolute if needed
                if src.startswith("//"):
                    src = "https:" + src
                logging.debug(f"Following embed src to search for m3u8: {src}")
                r2 = requests.get(src, timeout=HTTP_TIMEOUT, headers=headers)
                r2.raise_for_status()
                m2 = M3U8_RE.search(r2.text)
                if m2:
                    found2 = m2.group(1)
                    logging.info(f"Found m3u8 on embed: {found2}")
                    return found2
    except Exception as e:
        logging.debug(f"Iframe fetch error: {e}")
    return None

# ---------- PARSING & BUILDING ----------
def build_programs(data: Dict[str, Any], tz: ZoneInfo) -> (Dict[str, Dict], List[Dict]):
    """
    Returns channels dict and list of programmes.
    channels: {chan_id: {"display":..., "icon":...}}
    programmes: list of dict with keys: channel,start,stop,title,tag,category,poster,iframe,viewers,id,m3u8
    """
    channels = {}
    programmes = []

    for category in data.get("streams", []):
        for s in category.get("streams", []):
            uri = s.get("uri_name") or f"id_{s.get('id')}"
            chan_id = uri.replace("/", "_")
            poster = s.get("poster")
            if chan_id not in channels:
                channels[chan_id] = {
                    "display": s.get("tag") or s.get("category_name") or chan_id,
                    "icon": poster
                }
            start = s.get("starts_at")
            stop = s.get("ends_at")
            if start is None or stop is None:
                # skip if no times
                continue
            prog = {
                "channel": chan_id,
                "start": int(start),
                "stop": int(stop),
                "title": s.get("name") or f"stream_{s.get('id')}",
                "tag": s.get("tag"),
                "category": s.get("category_name"),
                "poster": poster,
                "iframe": s.get("iframe"),
                "viewers": s.get("viewers"),
                "id": s.get("id"),
                "m3u8": None
            }
            programmes.append(prog)

    logging.info(f"Parsed {len(channels)} channels and {len(programmes)} programmes")
    return channels, programmes

# ---------- CLEAN OVERLAPS ----------
def clean_overlaps(programmes: List[Dict], strategy: str = "trim") -> List[Dict]:
    """
    strategy:
      - "trim": shorten earlier programme to not overlap (end = next.start)
      - "merge": merge overlapping programmes into one (extend stop to max)
    """
    cleaned = []
    # group by channel
    by_chan = {}
    for p in programmes:
        by_chan.setdefault(p["channel"], []).append(p)

    for chan, plist in by_chan.items():
        plist.sort(key=lambda x: x["start"])
        merged = []
        cur = None
        for p in plist:
            if cur is None:
                cur = p.copy()
                continue
            if p["start"] < cur["stop"]:  # overlap
                if strategy == "trim":
                    # trim cur.stop to p.start (must ensure at least 1 second)
                    if p["start"] <= cur["start"]:
                        # completely overlapped: skip cur and take p as current
                        logging.debug(f"Complete overlap on {chan}: skipping prior {cur['title']}")
                        cur = p.copy()
                        continue
                    else:
                        logging.debug(f"Trimming {cur['title']} end {cur['stop']} -> {p['start']}")
                        cur["stop"] = p["start"]
                        merged.append(cur)
                        cur = p.copy()
                else:  # merge
                    logging.debug(f"Merging {cur['title']} with {p['title']} on {chan}")
                    cur["stop"] = max(cur["stop"], p["stop"])
                    # optionally combine titles/viewers etc.
                    cur["title"] = f"{cur['title']} / {p['title']}"
            else:
                merged.append(cur)
                cur = p.copy()
        if cur is not None:
            merged.append(cur)
        cleaned.extend(merged)
    logging.info(f"Cleaned overlaps: {len(programmes)} -> {len(cleaned)} programmes")
    # return sorted by start globally
    return sorted(cleaned, key=lambda x: (x["channel"], x["start"]))

# ---------- XMLTV GENERATION ----------
def generate_xmltv(channels: Dict[str, Dict], programmes: List[Dict], tz: ZoneInfo) -> ET.Element:
    tv = ET.Element("tv")
    tv.set("source-info-name", "ppv.to")
    tv.set("generator-info-name", "ppv_epg.py")

    # channels
    for cid, meta in channels.items():
        ch = ET.SubElement(tv, "channel", id=cid)
        dn = ET.SubElement(ch, "display-name")
        dn.text = safe_text(meta.get("display"))
        if meta.get("icon"):
            ico = ET.SubElement(ch, "icon")
            ico.set("src", meta.get("icon"))

    # programmes
    for p in sorted(programmes, key=lambda x: (x["channel"], x["start"])):
        pr = ET.SubElement(tv, "programme",
                           start=xmltv_time(p["start"], tz),
                           stop=xmltv_time(p["stop"], tz),
                           channel=p["channel"])
        t = ET.SubElement(pr, "title", lang="en")
        t.text = safe_text(p["title"])

        if p.get("tag"):
            st = ET.SubElement(pr, "sub-title", lang="en")
            st.text = safe_text(p["tag"])

        if p.get("category"):
            cat = ET.SubElement(pr, "category", lang="en")
            cat.text = safe_text(p["category"])

        # episode-num with stream id (user requested program icons in episode-num)
        epn = ET.SubElement(pr, "episode-num")
        epn.set("system", "urn:stream-id")
        epn.text = safe_text(p.get("id"))

        # duration - add as <length> element (XMLTV optional)
        length = ET.SubElement(pr, "length")
        length.set("units", "minutes")
        length.text = str(max(0, (p["stop"] - p["start"]) // 60))

        # desc includes iframe, viewers and poster
        desc = ET.SubElement(pr, "desc", lang="en")
        desc_parts = []
        if p.get("iframe"):
            desc_parts.append(f"embed={p['iframe']}")
        if p.get("viewers") is not None:
            desc_parts.append(f"viewers={p['viewers']}")
        if p.get("poster"):
            desc_parts.append(f"poster={p['poster']}")
        if p.get("m3u8"):
            desc_parts.append(f"hls={p['m3u8']}")
        desc.text = " — ".join(desc_parts)

        # icon inside programme
        if p.get("poster"):
            icon = ET.SubElement(pr, "icon")
            icon.set("src", p["poster"])

        # if m3u8 found, add <url>
        if p.get("m3u8"):
            url_el = ET.SubElement(pr, "url")
            url_el.text = p["m3u8"]

    return tv

# ---------- MAIN WORKFLOW ----------
def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate XMLTV EPG from ppv.to API")
    parser.add_argument("--tz", default="Asia/Manila", help="Timezone (default Asia/Manila)")
    parser.add_argument("--clean-strategy", choices=["trim", "merge"], default="trim",
                        help="How to handle overlapping programmes (default: trim)")
    parser.add_argument("--skip-hls", action="store_true", help="Skip fetching iframe pages for m3u8")
    parser.add_argument("--output-dir", default=".", help="Output directory")
    args = parser.parse_args(argv)

    try:
        tz = ZoneInfo(args.tz)
    except Exception:
        logging.warning("Timezone not found; falling back to Asia/Manila")
        tz = ZoneInfo("Asia/Manila")

    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    # fetch data
    data = fetch_api(API_URL)

    # parse channels/programmes
    channels, programmes = build_programs(data, tz)

    # attempt to fetch m3u8 for each programme (optionally skip)
    if not args.skip_hls:
        for p in programmes:
            if p.get("iframe"):
                try:
                    m3u8 = fetch_iframe_m3u8(p["iframe"])
                    if m3u8:
                        p["m3u8"] = m3u8
                except Exception as e:
                    logging.debug(f"HLS extraction error for {p.get('iframe')}: {e}")

    # write raw xmltv (before cleaning)
    xmltv_raw = generate_xmltv(channels, programmes, tz)
    raw_bytes = ET.tostring(xmltv_raw, encoding="utf-8")
    header = b'<?xml version="1.0" encoding="utf-8"?>\n'
    Path(outdir / EPG_XML).write_bytes(header + raw_bytes)
    write_gz(outdir / EPG_GZ, header + raw_bytes)
    logging.info(f"Wrote raw EPG: {outdir/EPG_XML}")

    # clean overlaps
    cleaned_programmes = clean_overlaps(programmes, strategy=args.clean_strategy)

    # generate cleaned xmltv
    xmltv_clean = generate_xmltv(channels, cleaned_programmes, tz)
    clean_bytes = ET.tostring(xmltv_clean, encoding="utf-8")
    Path(outdir / EPG_CLEAN_XML).write_bytes(header + clean_bytes)
    write_gz(outdir / EPG_CLEAN_GZ, header + clean_bytes)
    logging.info(f"Wrote cleaned EPG: {outdir/EPG_CLEAN_XML}")

if __name__ == "__main__":
    main()
