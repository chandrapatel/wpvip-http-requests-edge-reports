#!/usr/bin/env python3
"""
WPVIP Edge Log Report Generator
================================
Turns a month of WPVIP edge logs into reports. Logs keep the folder layout
WPVIP uses in S3: http_requests_edge/<YYYY>/<MM>/<DD>/*.gz. Reports go to a
reports/ folder next to http_requests_edge/:

  reports/<YYYY-MM>/report_<YYYY-MM>.html                 Overview of all hosts (one site or a multisite network)
  reports/<YYYY-MM>/sites/report_<YYYY-MM>_<site>.html    Full report for one site
  reports/<YYYY-MM>/report_<YYYY-MM>.json                 All monthly data
  reports/<YYYY-MM>/daily/report_<YYYY-MM-DD>.json.gz     One file per day, merged into the monthly data

Each day folder is processed once. Re-runs only process new days, days that
gained files, and days processed by an older version of this script. Reports
live outside the logs folder, so deleting logs keeps them.

Usage:
  python3 process_logs.py ~/wpvip-logs/http_requests_edge/2026/09/ --workers 3   # safe with 16 GB RAM
  python3 process_logs.py ~/wpvip-logs/http_requests_edge/2026/09/ --html-only
"""

import gzip
import heapq
import html
import itertools
import json
import re
import sys
import time
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qsl, unquote


# Bump when the daily report format changes, so old daily reports are reprocessed.
SCHEMA = 3

# x-cache values per https://docs.wpvip.com/caching/page-cache/http-headers/added-by-vip/
CACHE_SERVED  = {"HIT", "STALE", "UPDATING"}
ORIGIN_SERVED = {"MISS", "EXPIRED", "BYPASS", "REVALIDATED"}

# S3 layout: http_requests_edge/<YYYY>/<MM>/<DD>/
YEAR_DIR = re.compile(r"\d{4}")
DAY_DIR  = re.compile(r"\d{2}")

# Share of all requests a host needs to get its own site report.
SITE_MIN_SHARE = 0.001

# Metric vector kept for totals, categories, days and URLs.
REQUESTS, HUMAN, BOT, UNCLASSIFIED, CACHE, ORIGIN, BYTES, TIME = range(8)
VEC_LEN = 8

# Client vectors (ASNs, user agents, JA4, clients) add three crawler signals.
DATACENTER, SEARCHES, PAGES = range(8, 11)
CLIENT_VEC_LEN = 11

CATEGORIES = ("page", "search", "feed", "api", "sitemap", "asset", "other")
CATEGORY_LABELS = {
    "page": "Pages", "search": "Search", "feed": "Feeds", "api": "API",
    "sitemap": "Sitemaps", "asset": "Assets", "other": "Other",
}
CATEGORY_NOTES = {
    "page":    "HTML pages. Query strings removed.",
    "search":  "Search parameters only (s and ep_filter_*). Path searches like /search/term show as s=term.",
    "feed":    "RSS and Atom feeds.",
    "api":     "REST API (/wp-json/), admin-ajax.php and other JSON responses.",
    "sitemap": "XML sitemaps.",
    "asset":   "JS, CSS, images, fonts, audio, video and PDFs, including /_static/.",
    "other":   "Everything else, such as robots.txt and xmlrpc.php.",
}

# (rows kept per day, rows kept per month). Daily lists are deeper so the
# merged monthly lists stay accurate.
URL_DEPTH = {
    "page":    (3000, 500),
    "search":  (3000, 500),
    "feed":    (1000, 200),
    "api":     (1000, 200),
    "sitemap": (1000, 200),
    "asset":   (300, 50),
    "other":   (300, 50),
}
SLOW_DEPTH = (1000, 100)
TOP_DEPTH = {
    "countries":   (300, 50),
    "referrers":   (1000, 100),
    "ja3":         (500, 50),
    "errors_404":  (1000, 100),
    "errors_5xx":  (1000, 100),
}
# A client is one ASN + proxy type + traffic class + user agent combination.
CLIENT_DEPTH = {
    "asns":        (1000, 100),
    "user_agents": (2000, 200),
    "ja4":         (500, 50),
    "clients":     (3000, 500),
}
UA_MAX_LEN = 300
# Distinct user agents counted per JA4 fingerprint, per day.
JA4_UA_CAP = 1000

# Names for common ASNs. Others link to bgp.he.net for lookup.
ASN_NAMES = {
    "15169": "Google", "396982": "Google Cloud", "8075": "Microsoft", "16509": "Amazon AWS",
    "14618": "Amazon AWS", "13335": "Cloudflare", "54113": "Fastly", "20940": "Akamai",
    "36183": "Akamai", "63949": "Akamai (Linode)", "714": "Apple", "6185": "Apple", "32934": "Meta",
    "7922": "Comcast", "7018": "AT&T", "20057": "AT&T Mobility", "701": "Verizon",
    "6167": "Verizon Wireless", "22394": "Verizon Wireless", "21928": "T-Mobile USA",
    "20115": "Charter", "11351": "Charter", "10796": "Charter", "22773": "Cox", "5650": "Frontier",
    "209": "Lumen", "3356": "Lumen", "174": "Cogent", "6939": "Hurricane Electric",
    "14061": "DigitalOcean", "24940": "Hetzner", "16276": "OVH", "20473": "Vultr", "51167": "Contabo",
    "12876": "Scaleway", "36352": "ColoCrossing", "9009": "M247", "212238": "Datacamp",
    "45102": "Alibaba Cloud", "37963": "Alibaba", "132203": "Tencent Cloud", "45090": "Tencent",
    "136907": "Huawei Cloud", "4134": "China Telecom", "4837": "China Unicom", "55836": "Reliance Jio",
    "2914": "NTT", "3320": "Deutsche Telekom", "5089": "Virgin Media", "2856": "BT",
}

# User-agent tokens of well-known crawlers, used by the "claims to be a crawler" suspect rule.
CRAWLER_UA = re.compile(
    r"googlebot|bingbot|applebot|gptbot|oai-searchbot|chatgpt-user|claudebot|claude-web|perplexitybot|"
    r"ccbot|amazonbot|yandexbot|baiduspider|duckduckbot|facebookexternalhit|meta-externalagent|"
    r"bytespider|petalbot|semrushbot|ahrefsbot|mj12bot|dotbot",
    re.IGNORECASE,
)
BROWSER_VERSION = re.compile(r"(Chrome|Firefox)/(\d+)\.")
# Chrome and Firefox ship about 13 major versions a year, so 24 behind is roughly two years old.
OUTDATED_VERSIONS = 24
SEARCH_HEAVY_SHARE = 0.5
COUNT_KEYS = ("traffic_class", "x_cache", "status", "methods", "tls_versions",
              "http_versions", "mobile_class", "proxy_types", "hourly")

ASSET_EXT = {
    "js", "mjs", "css", "map", "png", "jpg", "jpeg", "gif", "webp", "avif", "svg", "ico", "bmp",
    "tif", "tiff", "woff", "woff2", "ttf", "otf", "eot", "mp3", "m4a", "wav", "ogg", "mp4", "mov",
    "webm", "m4v", "pdf", "zip",
}
ASSET_CT_PREFIX = ("image/", "audio/", "video/", "font/", "application/font", "application/x-font")
ASSET_CT = {"application/javascript", "text/javascript", "application/x-javascript", "text/css",
            "application/pdf", "application/vnd.ms-fontobject"}
FEED_CT  = {"application/rss+xml", "application/atom+xml", "application/rdf+xml"}


# ── Processing ────────────────────────────────────────────────────────────────

def field(e: dict, key: str, default: str = "Unknown") -> str:
    v = e.get(key)
    return str(v) if v not in (None, "") else default


def number(v, cast) -> float:
    try:
        return cast(v) if v not in (None, "", "-") else 0
    except (TypeError, ValueError):
        return 0


@lru_cache(maxsize=4096)
def normalize_host(host: str) -> str:
    host = host.lower().split(":", 1)[0].strip(".")
    return (host[4:] if host.startswith("www.") else host) or "unknown"


def referrer_domain(ref: str) -> str:
    if not ref or ref == "-":
        return "(none)"
    if not ref.startswith(("http://", "https://")):
        return "(other)"
    return normalize_host(ref.split("/", 3)[2].rsplit("@", 1)[-1])


def api_params(query: str) -> str:
    """Parameter names without values, so /?infinity=scrolling&page=3 groups as /?infinity&page."""
    if not query:
        return ""
    names = sorted({f"rest_route={v}" if k == "rest_route" else k
                    for k, v in parse_qsl(query, keep_blank_values=True)})
    return ("?" + "&".join(names))[:150] if names else ""


def classify(path: str, query: str, ctype: str) -> tuple:
    """Return (category, key). The key is the path, or the search parameters for a search."""
    if query and ("s=" in query or "ep_filter_" in query):
        params = sorted(
            (k, v.strip().lower()) for k, v in parse_qsl(query, keep_blank_values=True)
            if k == "s" or k.startswith("ep_filter_")
        )
        if params:
            return "search", "&".join(f"{k}={v}" for k, v in params)[:200]

    lower = path.lower()
    if lower.startswith("/search/"):
        return "search", "s=" + unquote(path[8:]).strip("/").lower()[:200]
    if "sitemap" in lower and lower.endswith((".xml", ".xsl", ".xml.gz")):
        return "sitemap", path

    # File extension wins over content type, so a 404 page for a missing .js still counts as an asset.
    name = lower.rsplit("/", 1)[-1]
    ext  = name.rsplit(".", 1)[1] if "." in name else ""
    if lower.startswith("/_static/") or ext in ASSET_EXT:
        return "asset", path
    if lower.startswith("/wp-json/") or lower.endswith("/admin-ajax.php") or "rest_route=" in query \
            or ctype == "application/json":
        return "api", path + api_params(query)
    if ctype in FEED_CT or lower.endswith(("/feed", "/feed/")) or "/feed/" in lower:
        return "feed", path
    if ctype.startswith(ASSET_CT_PREFIX) or ctype in ASSET_CT:
        return "asset", path
    if ctype == "text/html" or (not ctype and not ext):
        return "page", path
    return "other", path


def new_vec() -> list:
    return [0] * VEC_LEN


def new_client_vec() -> list:
    return [0] * CLIENT_VEC_LEN


def bump(vec: list, cls: int, cache: int, size: int, secs: float) -> None:
    vec[REQUESTS] += 1
    vec[cls]      += 1
    if cache:
        vec[cache] += 1
    vec[BYTES] += size
    vec[TIME]  += secs


def bump_client(vec: list, cls: int, cache: int, size: int, secs: float, datacenter: bool, category: str) -> None:
    bump(vec, cls, cache, size, secs)
    if datacenter:
        vec[DATACENTER] += 1
    if category == "search":
        vec[SEARCHES] += 1
    elif category == "page":
        vec[PAGES] += 1


def add_into(vec: list, other: list) -> None:
    for i in range(len(other)):
        vec[i] += other[i]


def rounded(vec: list) -> list:
    return [round(x, 3) if isinstance(x, float) else x for x in vec]


def top_vecs(d: dict, n: int) -> list:
    return [[k, rounded(v)] for k, v in heapq.nlargest(n, d.items(), key=lambda kv: kv[1][REQUESTS])]


class SiteStats:
    """All metrics for one site. Serialises to JSON and merges back from it."""

    def __init__(self):
        self.totals     = new_vec()
        self.categories = {c: new_vec() for c in CATEGORIES}
        self.daily      = defaultdict(new_vec)
        self.counts     = {k: Counter() for k in COUNT_KEYS}
        self.tops       = {k: Counter() for k in TOP_DEPTH}
        self.urls       = {c: defaultdict(new_vec) for c in CATEGORIES}
        self.slow       = defaultdict(new_vec)
        self.clients    = {k: defaultdict(new_client_vec) for k in CLIENT_DEPTH}
        self.ja4_uas    = defaultdict(set)   # while processing: JA4 → hashes of distinct user agents
        self.ja4_ua_max = Counter()          # after merging: JA4 → most distinct user agents in one day
        self.logged_in  = 0

    def to_json(self, level: int) -> dict:
        """level 0 = daily depth, 1 = monthly depth."""
        # Assets are left out: their request_time is mostly the client's download time, e.g. podcast MP3s.
        if self.slow:
            rows = ((c, k, v) for (c, k), v in self.slow.items())
        else:
            rows = ((c, k, v) for c, d in self.urls.items() if c != "asset" for k, v in d.items())
        slow = heapq.nlargest(SLOW_DEPTH[level], rows, key=lambda r: r[2][TIME])

        clients  = {k: top_vecs(self.clients[k], n[level]) for k, n in CLIENT_DEPTH.items()}
        ua_count = {k: len(v) for k, v in self.ja4_uas.items()} or self.ja4_ua_max

        return {
            "totals":     rounded(self.totals),
            "categories": {c: rounded(v) for c, v in self.categories.items() if v[REQUESTS]},
            "daily":      {d: rounded(v) for d, v in sorted(self.daily.items())},
            "counts":     {k: dict(c) for k, c in self.counts.items()},
            "tops":       {k: self.tops[k].most_common(n[level]) for k, n in TOP_DEPTH.items()},
            "urls":       {c: top_vecs(self.urls[c], URL_DEPTH[c][level]) for c in CATEGORIES if self.urls[c]},
            "slow":       [[c, k, rounded(v)] for c, k, v in slow],
            "clients":    clients,
            "ja4_uas":    {k: ua_count.get(k, 0) for k, _ in clients["ja4"]},
            "logged_in":  self.logged_in,
        }

    def merge(self, data: dict, with_urls: bool = True) -> None:
        add_into(self.totals, data["totals"])
        for c, v in data["categories"].items():
            add_into(self.categories[c], v)
        # A day folder can hold a few requests timestamped on a neighbouring day.
        for d, v in data["daily"].items():
            add_into(self.daily[d], v)
        for k, c in data["counts"].items():
            self.counts[k].update(c)
        for k, rows in data["tops"].items():
            for item, n in rows:
                self.tops[k][item] += n
        self.logged_in += data["logged_in"]
        for k, rows in data["clients"].items():
            for key, v in rows:
                add_into(self.clients[k][key], v)
        for k, n in data["ja4_uas"].items():
            self.ja4_ua_max[k] = max(self.ja4_ua_max[k], n)
        if with_urls:
            for c, rows in data["urls"].items():
                for key, v in rows:
                    add_into(self.urls[c][key], v)
            for c, key, v in data["slow"]:
                add_into(self.slow[(c, key)], v)


def process_day(day_dir: Path, period: str, show_progress: bool = False) -> dict:
    gz_files = sorted(day_dir.rglob("*.gz"))
    sites    = defaultdict(SiteStats)
    total    = 0

    for i, gz_file in enumerate(gz_files, 1):
        if show_progress:
            pct = i / len(gz_files) * 100
            print(f"  [{pct:5.1f}%] {period}  {gz_file.name[:50]:<50}", end="\r")
        try:
            with gzip.open(gz_file, "rt", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        e = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(e, dict):
                        continue

                    total += 1
                    s      = sites[normalize_host(field(e, "http_host", ""))]
                    tc     = field(e, "traffic_class")
                    xcache = field(e, "sent_x_cache", "UNKNOWN").upper()
                    status = field(e, "status")
                    ts     = field(e, "timestamp_iso8601", "")
                    ctype  = field(e, "content_type", "").split(";", 1)[0].strip().lower()
                    path, _, query = field(e, "request_url", "/").partition("?")
                    category, key  = classify(path or "/", query, ctype)

                    cls   = HUMAN if tc == "People" else UNCLASSIFIED if tc == "Unknown" else BOT
                    cache = CACHE if xcache in CACHE_SERVED else ORIGIN if xcache in ORIGIN_SERVED else 0
                    size  = number(e.get("body_bytes_sent"), int)
                    secs  = number(e.get("request_time"), float)
                    day   = ts[:10] if len(ts) >= 10 else "unknown"

                    bump(s.totals, cls, cache, size, secs)
                    bump(s.categories[category], cls, cache, size, secs)
                    bump(s.daily[day], cls, cache, size, secs)
                    bump(s.urls[category][key], cls, cache, size, secs)

                    c = s.counts
                    c["traffic_class"][tc]                            += 1
                    c["x_cache"][xcache]                              += 1
                    c["status"][status]                               += 1
                    c["methods"][field(e, "request_type")]            += 1
                    c["tls_versions"][field(e, "tls_version")]        += 1
                    c["http_versions"][field(e, "http_version")]      += 1
                    c["mobile_class"][field(e, "mobile_class")]       += 1
                    c["proxy_types"][field(e, "http_x_ip_proxy_type", "-")] += 1
                    if ts[11:13].isdigit():
                        c["hourly"][int(ts[11:13])] += 1

                    asn   = field(e, "asn")
                    ua    = field(e, "http_user_agent")[:UA_MAX_LEN]
                    proxy = field(e, "http_x_ip_proxy_type", "-")
                    ja4   = field(e, "tls_ja4_hash", "")
                    dch   = proxy == "DCH"
                    cl    = s.clients
                    bump_client(cl["asns"][asn], cls, cache, size, secs, dch, category)
                    bump_client(cl["user_agents"][ua], cls, cache, size, secs, dch, category)
                    bump_client(cl["clients"][f"{asn}\t{proxy}\t{tc}\t{ua}"], cls, cache, size, secs, dch, category)
                    if ja4:
                        bump_client(cl["ja4"][ja4], cls, cache, size, secs, dch, category)
                        uas = s.ja4_uas[ja4]
                        if len(uas) < JA4_UA_CAP:
                            uas.add(hash(ua))

                    t = s.tops
                    t["countries"][field(e, "upstream_country_code")]  += 1
                    t["referrers"][referrer_domain(field(e, "http_referer", ""))] += 1
                    ja3 = field(e, "tls_ja3_hash", "")
                    if ja3: t["ja3"][ja3] += 1
                    if status == "404":
                        t["errors_404"][key] += 1
                    elif status.startswith("5"):
                        t["errors_5xx"][key] += 1

                    # Count logged-in requests only; wplogin holds real usernames.
                    if field(e, "wplogin", "-") != "-":
                        s.logged_in += 1

        except Exception as ex:
            print(f"\n  ⚠ Skipped {period}/{gz_file.name}: {ex}")

    if show_progress:
        print(" " * 80, end="\r")

    return {
        "meta": {
            "schema":          SCHEMA,
            "period":          period,
            "log_dir":         str(day_dir),
            "generated_at":    now_utc(),
            "files_processed": len(gz_files),
            "total_requests":  total,
        },
        "sites": {host: stats.to_json(0) for host, stats in sites.items()},
    }


def run_day(day_dir: Path, period: str, out_path: Path, show_progress: bool = False) -> tuple:
    """Process one day folder and write its daily report. Runs in a worker process."""
    start  = time.monotonic()
    report = process_day(day_dir, period, show_progress)
    write_json(out_path, report)
    meta = report["meta"]
    return period, meta["total_requests"], meta["files_processed"], time.monotonic() - start


def merge_days(daily_paths: list, period: str, log_dir: Path) -> dict:
    sites   = defaultdict(SiteStats)
    network = SiteStats()
    days = files = 0

    for path in daily_paths:
        r = read_json(path)
        if not r or r["meta"].get("schema") != SCHEMA:
            print(f"  ⚠ Ignored {path.name}: missing or from an older script version")
            continue
        days  += 1
        files += r["meta"]["files_processed"]
        for host, data in r["sites"].items():
            sites[host].merge(data)
            network.merge(data, with_urls=False)

    total = network.totals[REQUESTS]
    main  = {h: s for h, s in sites.items() if s.totals[REQUESTS] >= total * SITE_MIN_SHARE}

    return {
        "meta": {
            "schema":          SCHEMA,
            "period":          period,
            "log_dir":         str(log_dir),
            "generated_at":    now_utc(),
            "days_processed":  days,
            "files_processed": files,
            "total_requests":  total,
        },
        "network":     network.to_json(1),
        "sites":       {h: s.to_json(1) for h, s in sorted(main.items(), key=lambda x: -x[1].totals[REQUESTS])},
        "other_hosts": {h: rounded(s.totals) for h, s in sites.items() if h not in main},
    }


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: Path, data: dict) -> None:
    """Write via a temp file so an interrupted run never leaves a half-written report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    if path.suffix == ".gz":
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
    else:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=1)
    tmp.replace(path)


def read_json(path: Path):
    try:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, EOFError, json.JSONDecodeError):
        return None


# ── HTML report ───────────────────────────────────────────────────────────────

CHART_JS = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"

CSS = """
  :root {
    --navy: #0D1B2A; --blue: #1A56DB; --lt-blue: #EBF2FF;
    --red: #EF4444; --amber: #F59E0B; --green: #10B981; --purple: #8B5CF6; --teal: #14B8A6;
    --gray-1: #F3F4F6; --gray-3: #D1D5DB; --gray-4: #9CA3AF; --gray-6: #4B5563;
    --radius: 10px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         background: #F8FAFC; color: var(--navy); font-size: 14px; line-height: 1.5; }
  a { color: var(--blue); }
  header { background: var(--navy); color: #fff; padding: 24px 32px; }
  header h1 { font-size: 22px; font-weight: 700; }
  header p  { font-size: 12px; color: #93C5FD; margin-top: 4px; }
  header a  { color: #93C5FD; }
  .wrap { max-width: 1280px; margin: 0 auto; padding: 24px; }
  h2.section { font-size: 16px; margin: 28px 0 12px; padding-bottom: 6px; border-bottom: 2px solid var(--gray-3); }
  .kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 14px; margin-bottom: 16px; }
  .kpi { background: #fff; border: 1px solid var(--gray-3); border-radius: var(--radius); padding: 14px 16px; }
  .kpi .label { font-size: 11px; color: var(--gray-6); text-transform: uppercase; letter-spacing: .04em; margin-bottom: 4px; }
  .kpi .value { font-size: 24px; font-weight: 700; }
  .kpi .sub   { font-size: 12px; color: var(--gray-6); margin-top: 2px; }
  .kpi.blue   { border-top: 3px solid var(--blue); }
  .kpi.green  { border-top: 3px solid var(--green); }
  .kpi.red    { border-top: 3px solid var(--red); }
  .kpi.gray   { border-top: 3px solid var(--gray-4); }
  .kpi.amber  { border-top: 3px solid var(--amber); }
  .kpi.purple { border-top: 3px solid var(--purple); }
  .kpi.teal   { border-top: 3px solid var(--teal); }
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
  .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-bottom: 16px; }
  .card { background: #fff; border: 1px solid var(--gray-3); border-radius: var(--radius); padding: 16px 18px; margin-bottom: 16px; min-width: 0; }
  .grid-2 .card, .grid-3 .card { margin-bottom: 0; }
  .card h3 { font-size: 13px; font-weight: 600; color: var(--gray-6); text-transform: uppercase; letter-spacing: .04em; margin-bottom: 4px; }
  .card .note { font-size: 12px; color: var(--gray-6); margin-bottom: 10px; }
  .chart-wrap { position: relative; height: 240px; }
  .chart-wrap.tall { height: 300px; }
  .filter { width: 100%; padding: 6px 10px; margin-bottom: 8px; border: 1px solid var(--gray-3); border-radius: 6px; font-size: 13px; }
  .scroll { overflow: auto; max-height: 440px; }
  .scroll.short { max-height: 260px; }
  table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
  th { position: sticky; top: 0; background: var(--navy); color: #fff; padding: 7px 10px; text-align: left;
       font-size: 11px; font-weight: 600; cursor: pointer; white-space: nowrap; user-select: none; }
  th.num { color: #fff; }
  th[data-dir="asc"]::after  { content: " ▲"; }
  th[data-dir="desc"]::after { content: " ▼"; }
  td { padding: 5px 10px; border-bottom: 1px solid var(--gray-1); color: var(--gray-6); vertical-align: top; }
  tr:hover td { background: var(--lt-blue); }
  .num { text-align: right; color: var(--navy); font-variant-numeric: tabular-nums; white-space: nowrap; }
  .mono { font-family: "SF Mono", Menlo, monospace; font-size: 11px; word-break: break-all; }
  .empty { text-align: center; color: var(--gray-4); }
  details summary { cursor: pointer; font-size: 13px; color: var(--gray-6); margin: 4px 0 10px; }
  dl.facts { display: grid; grid-template-columns: auto 1fr; gap: 4px 16px; font-size: 13px; }
  dl.facts dt { color: var(--gray-6); }
  dl.facts dd { text-align: right; font-variant-numeric: tabular-nums; }
  footer { text-align: center; font-size: 11px; color: var(--gray-6); padding: 20px; border-top: 1px solid var(--gray-3); margin-top: 8px; }
  @media (max-width: 900px) {
    .grid-2, .grid-3 { grid-template-columns: 1fr; }
    .wrap { padding: 16px; }
  }
"""

JS = """
const R = REPORT, tick = "#6B7280", grid = "#e5e7eb";
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
Chart.defaults.font.size = 11;
const short = v => v >= 1e9 ? (v / 1e9).toFixed(1) + "B" : v >= 1e6 ? (v / 1e6).toFixed(1) + "M" : v >= 1e3 ? (v / 1e3).toFixed(0) + "K" : v;
const stacked = {
  responsive: true, maintainAspectRatio: false,
  plugins: { legend: { position: "top" } },
  scales: {
    x: { stacked: true, grid: { display: false }, ticks: { color: tick, maxRotation: 45 } },
    y: { stacked: true, grid: { color: grid }, ticks: { color: tick, callback: short } }
  }
};
const series = (h, b, u) => [
  { label: "Human", data: h, backgroundColor: "#3B82F6", stack: "s" },
  { label: "Bot / Crawler", data: b, backgroundColor: "#EF4444", stack: "s" },
  { label: "Unclassified", data: u, backgroundColor: "#9CA3AF", stack: "s" },
];
new Chart(document.getElementById("dailyChart"), {
  type: "bar", data: { labels: R.days, datasets: series(R.dailyHuman, R.dailyBot, R.dailyUnclassified) }, options: stacked
});
new Chart(document.getElementById("categoryChart"), {
  type: "bar", data: { labels: R.catLabels, datasets: series(R.catHuman, R.catBot, R.catUnclassified) }, options: stacked
});
new Chart(document.getElementById("tcChart"), {
  type: "bar",
  data: { labels: R.tcLabels, datasets: [{ data: R.tcValues, backgroundColor: R.tcColors, borderRadius: 3 }] },
  options: { indexAxis: "y", responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } },
             scales: { x: { grid: { color: grid }, ticks: { color: tick, callback: short } },
                       y: { grid: { display: false }, ticks: { color: tick, autoSkip: false, font: { size: 10 } } } } }
});
new Chart(document.getElementById("hourlyChart"), {
  type: "bar",
  data: { labels: [...Array(24).keys()].map(h => String(h).padStart(2, "0") + ":00"),
          datasets: [{ data: R.hourly, backgroundColor: "#6366F1", borderRadius: 3 }] },
  options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } },
             scales: { x: { grid: { display: false }, ticks: { color: tick, maxRotation: 45 } },
                       y: { grid: { color: grid }, ticks: { color: tick, callback: short } } } }
});

// Table filter boxes
document.querySelectorAll("input[data-filter]").forEach(input => {
  const rows = [...document.getElementById(input.dataset.filter).tBodies[0].rows];
  input.addEventListener("input", () => {
    const q = input.value.trim().toLowerCase();
    rows.forEach(r => { r.hidden = q !== "" && !r.textContent.toLowerCase().includes(q); });
  });
});

// Click a column header to sort; numeric cells sort by data-v
document.querySelectorAll("table.sortable").forEach(table => {
  table.querySelectorAll("th").forEach((th, i) => {
    th.addEventListener("click", () => {
      const dir = th.dataset.dir === "desc" ? "asc" : "desc";
      table.querySelectorAll("th").forEach(h => delete h.dataset.dir);
      th.dataset.dir = dir;
      const val = r => { const c = r.cells[i]; return c.dataset.v !== undefined ? parseFloat(c.dataset.v) : c.textContent.toLowerCase(); };
      const sign = dir === "asc" ? 1 : -1;
      const body = table.tBodies[0];
      body.append(...[...body.rows].sort((a, b) => { const x = val(a), y = val(b); return (x < y ? -1 : x > y ? 1 : 0) * sign; }));
    });
  });
});
"""

FOOTER = (
    "WPVIP edge logs (http_requests_edge) &nbsp;·&nbsp; "
    "Cache hit = HIT, STALE, UPDATING; origin = MISS, EXPIRED, BYPASS, REVALIDATED &nbsp;·&nbsp; "
    "Unclassified = no WPVIP traffic class"
)

VEC_HEADERS = [("Requests", True), ("% of total", True), ("Human", True), ("Bot", True),
               ("Cache hit", True), ("Avg time", True), ("Bandwidth", True)]

_table_ids = itertools.count()


def js(value) -> str:
    """JSON for inline <script>; escapes "<" so data cannot close the tag."""
    return json.dumps(value).replace("<", "\\u003c")


def esc(value) -> str:
    return html.escape(str(value))


def pct(part, whole, digits: int = 1) -> str:
    return f"{part / whole * 100:.{digits}f}%" if whole else "—"


def ratio(part, whole) -> float:
    return round(part / whole, 5) if whole else 0


def cache_rate(v: list):
    served = v[CACHE] + v[ORIGIN]
    return v[CACHE] / served if served else None


def short_num(n) -> str:
    for size, unit in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if n >= size:
            return f"{n / size:.1f}{unit}"
    return f"{n:,}"


def fmt_bytes(n) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024


def fmt_secs(s) -> str:
    if s < 60:
        return f"{s:.1f} s"
    if s < 3600:
        return f"{s / 60:.1f} min"
    return f"{s / 3600:,.1f} h"


def fmt_ms(v: list) -> str:
    return f"{v[TIME] / v[REQUESTS] * 1000:,.0f} ms" if v[REQUESTS] else "—"


def td(text, sort=None, cls: str = "") -> str:
    """text must already be escaped."""
    attrs = f' class="{cls}"' if cls else ""
    if sort is not None:
        attrs += f' data-v="{sort}"'
    return f"<td{attrs}>{text}</td>"


def vec_cells(v: list, total) -> str:
    n    = v[REQUESTS]
    rate = cache_rate(v)
    return "".join((
        td(f"{n:,}", n, "num"),
        td(pct(n, total, 2), n, "num"),
        td(pct(v[HUMAN], n), ratio(v[HUMAN], n), "num"),
        td(pct(v[BOT], n), ratio(v[BOT], n), "num"),
        td("—" if rate is None else f"{rate * 100:.1f}%", -1 if rate is None else round(rate, 5), "num"),
        td(fmt_ms(v), ratio(v[TIME], n), "num"),
        td(fmt_bytes(v[BYTES]), v[BYTES], "num"),
    ))


def table(headers: list, rows: list, search: bool = False, short: bool = False) -> str:
    tid  = f"t{next(_table_ids)}"
    head = "".join(f'<th class="num">{h}</th>' if numeric else f"<th>{h}</th>" for h, numeric in headers)
    body = "".join(rows) or f'<tr><td colspan="{len(headers)}" class="empty">No data</td></tr>'
    filt = f'<input class="filter" type="search" placeholder="Filter rows…" data-filter="{tid}">' if search and rows else ""
    return (f'{filt}<div class="scroll{" short" if short else ""}"><table id="{tid}" class="sortable">'
            f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>")


def card(title: str, body: str, note: str = "") -> str:
    note_html = f'<p class="note">{note}</p>' if note else ""
    return f'<div class="card"><h3>{title}</h3>{note_html}{body}</div>'


def count_table(items, total, first: str, mono: bool = False, search: bool = True, short: bool = False) -> str:
    cls  = "mono" if mono else ""
    rows = [f"<tr>{td(esc(k), cls=cls)}{td(f'{n:,}', n, 'num')}{td(pct(n, total, 2), n, 'num')}</tr>"
            for k, n in items]
    return table([(first, False), ("Requests", True), ("% of total", True)], rows, search, short)


def chart_data(s: dict, period: str) -> dict:
    days = [d for d in sorted(s["daily"]) if d.startswith(period)]
    tc   = sorted(s["counts"]["traffic_class"].items(), key=lambda x: -x[1])
    others = [k for k, _ in tc if k not in ("People", "Unknown")]
    fixed  = {"People": "#3B82F6", "Unknown": "#9CA3AF"}
    cats   = [c for c in CATEGORIES if c in s["categories"]]
    return {
        "days":              days,
        "dailyHuman":        [s["daily"][d][HUMAN] for d in days],
        "dailyBot":          [s["daily"][d][BOT] for d in days],
        "dailyUnclassified": [s["daily"][d][UNCLASSIFIED] for d in days],
        "tcLabels":          ["Unclassified" if k == "Unknown" else k for k, _ in tc],
        "tcValues":          [n for _, n in tc],
        "tcColors":          [fixed.get(k) or f"hsl({others.index(k) * 360 // max(len(others), 1)}, 65%, 55%)"
                              for k, _ in tc],
        "catLabels":         [CATEGORY_LABELS[c] for c in cats],
        "catHuman":          [s["categories"][c][HUMAN] for c in cats],
        "catBot":            [s["categories"][c][BOT] for c in cats],
        "catUnclassified":   [s["categories"][c][UNCLASSIFIED] for c in cats],
        "hourly":            [s["counts"]["hourly"].get(str(h), 0) for h in range(24)],
    }


def kpi_row(s: dict) -> str:
    v    = s["totals"]
    n    = v[REQUESTS]
    rate = cache_rate(v)
    items = [
        ("blue",   "Total Requests",    short_num(n),             f"{n:,} requests"),
        ("green",  "Human",             pct(v[HUMAN], n),         f"{v[HUMAN]:,} requests"),
        ("red",    "Bot / Crawler",     pct(v[BOT], n),           f"{v[BOT]:,} requests"),
        ("gray",   "Unclassified",      pct(v[UNCLASSIFIED], n),  f"{v[UNCLASSIFIED]:,} requests"),
        ("amber",  "Cache Hit Rate",    "—" if rate is None else f"{rate * 100:.1f}%",
                                        f"{v[CACHE]:,} cache / {v[ORIGIN]:,} origin"),
        ("purple", "Bandwidth",         fmt_bytes(v[BYTES]),      f"{fmt_bytes(v[BYTES] / n) if n else '—'} per request"),
        ("teal",   "Avg Response Time", fmt_ms(v),                f"{fmt_secs(v[TIME])} in total"),
    ]
    return '<div class="kpi-row">' + "".join(
        f'<div class="kpi {color}"><div class="label">{label}</div><div class="value">{value}</div>'
        f'<div class="sub">{sub}</div></div>'
        for color, label, value, sub in items
    ) + "</div>"


def overview_sections(s: dict) -> str:
    total = s["totals"][REQUESTS]
    cat_rows = [f"<tr>{td(CATEGORY_LABELS[c])}{vec_cells(s['categories'][c], total)}</tr>"
                for c in CATEGORIES if c in s["categories"]]
    notes = " ".join(f"<b>{CATEGORY_LABELS[c]}</b>: {esc(CATEGORY_NOTES[c])}" for c in CATEGORIES)
    return (
        kpi_row(s)
        + card("Daily Requests", '<div class="chart-wrap tall"><canvas id="dailyChart"></canvas></div>',
               "Requests per day, split into human, bot and unclassified. Days are in UTC.")
        + '<div class="grid-3">'
        + card("Traffic Class", '<div class="chart-wrap tall"><canvas id="tcChart"></canvas></div>',
               "Requests per WPVIP traffic class. Hover a bar for the exact count.")
        + card("Requests by Category", '<div class="chart-wrap tall"><canvas id="categoryChart"></canvas></div>',
               "Requests per category, split into human, bot and unclassified. The Categories table defines each one.")
        + card("Requests by Hour (UTC)", '<div class="chart-wrap tall"><canvas id="hourlyChart"></canvas></div>',
               "Requests per hour of the day, summed over the period. US Eastern time is UTC−4 in summer and "
               "UTC−5 in winter.")
        + "</div>"
        + card("Categories", table([("Category", False)] + VEC_HEADERS, cat_rows), notes)
    )


def source_sections(s: dict) -> str:
    total = s["totals"][REQUESTS]
    tops  = s["tops"]
    return (
        '<h2 class="section">Traffic Sources</h2>'
        + '<div class="grid-2">'
        + card("Referrer Domains", count_table(tops["referrers"], total, "Domain"),
               "(none) means no referrer. Only the domain is kept.")
        + card("Countries", count_table(tops["countries"], total, "Country"),
               "Country of the visitor's IP address, from WPVIP's upstream_country_code field. VPN users show "
               "the country of their VPN exit.")
        + "</div>"
    )


CLIENT_HEADERS = [("Requests", True), ("% of total", True), ("Human", True), ("Bot", True),
                  ("Unclassified", True), ("Datacenter IP", True), ("Search", True), ("Pages", True),
                  ("Cache hit", True), ("Bandwidth", True)]
CLIENT_HEADERS_SHORT = [("Requests", True), ("% of total", True), ("Search", True), ("Pages", True),
                        ("Cache hit", True), ("Bandwidth", True)]

# CDN networks that carry iCloud Private Relay, so their datacenter IPs are often real readers.
RELAY_ASNS = {"13335", "54113", "36183", "20940"}


def share_cell(part, n) -> str:
    return td(pct(part, n), ratio(part, n), "num")


def cache_cell(v: list) -> str:
    rate = cache_rate(v)
    return td("—" if rate is None else f"{rate * 100:.1f}%", -1 if rate is None else round(rate, 5), "num")


def client_cells(v: list, total, classes: bool = True) -> str:
    n     = v[REQUESTS]
    cells = [td(f"{n:,}", n, "num"), td(pct(n, total, 2), n, "num")]
    if classes:
        cells += [share_cell(v[HUMAN], n), share_cell(v[BOT], n), share_cell(v[UNCLASSIFIED], n),
                  share_cell(v[DATACENTER], n)]
    cells += [share_cell(v[SEARCHES], n), share_cell(v[PAGES], n), cache_cell(v), td(fmt_bytes(v[BYTES]), v[BYTES], "num")]
    return "".join(cells)


def asn_label(asn: str) -> str:
    if not asn.isdigit():
        return esc(asn)
    name = ASN_NAMES.get(asn)
    link = f'<a href="https://bgp.he.net/AS{asn}" target="_blank" rel="noopener">AS{asn}</a>'
    return f"{link} {esc(name)}" if name else link


def class_label(tc: str) -> str:
    return "Unclassified" if tc == "Unknown" else tc


def latest_versions(ua_rows: list) -> dict:
    """Newest Chrome and Firefox major versions among the busiest user agents."""
    latest = {}
    for ua, _ in ua_rows[:50]:
        m = BROWSER_VERSION.search(ua)
        if m:
            latest[m.group(1)] = max(latest.get(m.group(1), 0), int(m.group(2)))
    return latest


def suspect_reasons(asn: str, proxy: str, tc: str, ua: str, v: list, latest: dict) -> list:
    reasons    = []
    unverified = tc in ("People", "Unknown")
    if proxy == "DCH" and tc == "Unknown" and asn not in RELAY_ASNS:
        reasons.append("Unclassified traffic from datacenter IPs")
    if unverified and CRAWLER_UA.search(ua):
        reasons.append("Crawler user agent that WPVIP does not class as a bot")
    m = BROWSER_VERSION.search(ua)
    if unverified and m and m.group(1) in latest and int(m.group(2)) <= latest[m.group(1)] - OUTDATED_VERSIONS:
        reasons.append(f"Outdated browser ({m.group(1)} {m.group(2)})")
    if v[REQUESTS] and v[SEARCHES] / v[REQUESTS] >= SEARCH_HEAVY_SHARE:
        reasons.append("Mostly search requests")
    return reasons


def crawler_sections(s: dict) -> str:
    total   = s["totals"][REQUESTS]
    cl      = s["clients"]
    latest  = latest_versions(cl["user_agents"])
    ja4_uas = s["ja4_uas"]

    client_rows, suspect_rows = [], []
    for key, v in cl["clients"]:
        asn, proxy, tc, ua = key.split("\t", 3)
        cells = (f"{td(asn_label(asn))}{td(esc(proxy))}{td(esc(class_label(tc)))}"
                 f"{td(esc(ua), cls='mono')}{client_cells(v, total, classes=False)}")
        client_rows.append(f"<tr>{cells}</tr>")
        reasons = suspect_reasons(asn, proxy, tc, ua, v, latest)
        if reasons:
            suspect_rows.append(f"<tr>{td(esc('; '.join(reasons)))}{cells}</tr>")

    client_headers = [("ASN", False), ("Proxy", False), ("Class", False), ("User agent", False)] + CLIENT_HEADERS_SHORT
    asn_rows = [f"<tr>{td(asn_label(k))}{client_cells(v, total)}</tr>" for k, v in cl["asns"]]
    ua_rows  = [f"<tr>{td(esc(k), cls='mono')}{client_cells(v, total)}</tr>" for k, v in cl["user_agents"]]
    ja4_rows = [
        f"<tr>{td(esc(k), cls='mono')}{td(f'{ja4_uas.get(k, 0):,}', ja4_uas.get(k, 0), 'num')}{client_cells(v, total)}</tr>"
        for k, v in cl["ja4"]
    ]
    latest_note = ", ".join(f"{b} {n}" for b, n in sorted(latest.items())) or "none found"

    return (
        '<h2 class="section">Crawler Analysis</h2>'
        + '<p class="note">Look for rows with a high unclassified or datacenter IP share, mostly search or page '
          "requests, and a low cache hit rate. Proxy type comes from WPVIP's http_x_ip_proxy_type field: DCH appears "
          'on datacenter and hosting networks such as AWS and Alibaba Cloud, and "-" means no value.</p>'
        + card(f"Suspect Clients ({len(suspect_rows)})",
               table([("Reason", False)] + client_headers, suspect_rows, search=True),
               "Checked against the top clients below. Rules: unclassified traffic from datacenter IPs, leaving out "
               "the CDNs that carry iCloud Private Relay (Cloudflare, Fastly, Akamai); a well-known crawler user agent "
               f"that WPVIP classes as People or leaves unclassified; a Chrome or Firefox version {OUTDATED_VERSIONS} "
               f"or more behind the newest seen ({esc(latest_note)}); at least {SEARCH_HEAVY_SHARE:.0%} search "
               "requests. These are leads to check, not proof.")
        + card(f"Top Clients ({len(client_rows)})", table(client_headers, client_rows, search=True),
               "A client is one combination of ASN, proxy type, WPVIP traffic class and user agent.")
        + card(f"ASNs ({len(asn_rows)})", table([("ASN", False)] + CLIENT_HEADERS, asn_rows, search=True),
               "ASN names cover common networks only. Click a number to look it up.")
        + card(f"User Agents ({len(ua_rows)})", table([("User agent", False)] + CLIENT_HEADERS, ua_rows, search=True),
               "The user agent string the client sent, up to 300 characters. Any client can send any string, so "
               "check the unclassified and datacenter IP shares before trusting a browser name.")
        + card(f"TLS Fingerprints, JA4 ({len(ja4_rows)})",
               table([("JA4 hash", False), ("User agents", True)] + CLIENT_HEADERS, ja4_rows, search=True),
               "User agents = most distinct user agents seen with this fingerprint in one day, up to "
               f"{JA4_UA_CAP:,}. One fingerprint with many user agents often means one bot rotating its user agent.")
        + card("TLS Fingerprints, JA3", count_table(s["tops"]["ja3"], total, "JA3 hash", mono=True),
               "JA3 is an older TLS client fingerprint. It is less reliable than JA4: modern browsers such as Chrome "
               "shuffle their TLS extensions, so one browser can produce many JA3 hashes. Use it to match hashes "
               "from blocklists or other tools that still use JA3.")
    )


def technical_sections(s: dict, facts: list) -> str:
    total  = s["totals"][REQUESTS]
    counts = s["counts"]

    def counted(key, first, note, rename=None):
        items = sorted(counts[key].items(), key=lambda x: -x[1])
        if rename:
            items = [(rename.get(k, k), n) for k, n in items]
        return card(first, count_table(items, total, first, search=False, short=True), note)

    fact_rows = "".join(f"<dt>{esc(k)}</dt><dd>{esc(v)}</dd>" for k, v in facts)
    return (
        '<h2 class="section">Technical Details</h2><div class="grid-3">'
        + counted("status", "Status Code",
                  "HTTP status sent to the visitor: 2xx success, 3xx redirect (304 = not modified), "
                  "4xx client error, 5xx server error.")
        + counted("x_cache", "x-cache Value",
                  "Raw WPVIP cache result. The hit rate counts HIT, STALE and UPDATING as cache, and MISS, "
                  "EXPIRED, BYPASS and REVALIDATED as origin. UNKNOWN means the field was empty.")
        + counted("traffic_class", "Traffic Class",
                  "WPVIP's own classification of each request. Unclassified means WPVIP left it empty.",
                  {"Unknown": "Unclassified"})
        + counted("methods", "HTTP Method",
                  "GET fetches a page. HEAD checks a page without its body, often for monitors and crawlers. "
                  "POST sends data, such as forms and logins. Unusual methods such as PROPFIND usually come "
                  "from scanners.")
        + counted("tls_versions", "TLS Version",
                  "Encryption protocol the client used. TLS 1.3 is current. Older versions often point to "
                  "old software or scripts.")
        + counted("http_versions", "HTTP Version",
                  "Modern browsers mostly use HTTP/2. HTTP/1.1 and HTTP/1.0 are common for bots, scripts "
                  "and old clients.")
        + counted("mobile_class", "Device Class",
                  "WPVIP's mobile_class field: desktop, smart for smartphones, tablet, and dumb for basic phones.")
        + counted("proxy_types", "Proxy Type",
                  "WPVIP's http_x_ip_proxy_type field. In these logs: UNK appears mostly on home internet "
                  "providers such as Comcast and AT&T, so it usually means no proxy was detected. DCH appears on "
                  "datacenter and hosting networks such as AWS and Alibaba Cloud. SES appears on crawler networks "
                  "of Amazon, Microsoft, Google and Apple. VPN appears mostly on Cloudflare. RES appears on mobile "
                  "and home networks such as T-Mobile. PUB, TOR and WEB are rare and likely mark public proxies, "
                  "Tor exit nodes and web proxies.")
        + card("Report Facts", f'<dl class="facts">{fact_rows}</dl>', "Details of the data behind this report.")
        + "</div>"
    )


def page(title: str, subtitle: str, body: str, data: dict) -> str:
    return (
        '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{title}</title>\n"
        f'<script src="{CHART_JS}"></script>\n'
        f"<style>{CSS}</style>\n</head>\n<body>\n"
        f"<header><h1>{title}</h1><p>{subtitle}</p></header>\n"
        f'<div class="wrap">\n{body}\n</div>\n'
        f"<footer>{FOOTER}</footer>\n"
        f"<script>const REPORT = {js(data)};</script>\n<script>{JS}</script>\n"
        "</body>\n</html>\n"
    )


def site_filename(period: str, host: str) -> str:
    return f"report_{period}_{re.sub(r'[^a-z0-9.-]', '_', host)}.html"


def generated(meta: dict) -> str:
    return meta["generated_at"][:19].replace("T", " ") + " UTC"


def build_network_html(report: dict) -> str:
    meta, net = report["meta"], report["network"]
    period    = meta["period"]
    total     = net["totals"][REQUESTS]

    site_rows = []
    for h, s in report["sites"].items():
        link = f'<a href="sites/{esc(site_filename(period, h))}">{esc(h)}</a>'
        site_rows.append(f"<tr>{td(link)}{vec_cells(s['totals'], total)}</tr>")
    other = sorted(report["other_hosts"].items(), key=lambda x: -x[1][REQUESTS])
    other_rows = [f"<tr>{td(esc(h), cls='mono')}{vec_cells(v, total)}</tr>" for h, v in other]
    other_total = sum(v[REQUESTS] for _, v in other)
    other_card  = card(
        f"Other Hosts ({len(other)})",
        f"<details><summary>{other_total:,} requests ({pct(other_total, total, 2)}) from hosts below "
        f"{SITE_MIN_SHARE * 100:g}% of traffic, such as typos and IP addresses</summary>"
        + table([("Host", False)] + VEC_HEADERS, other_rows, search=True) + "</details>",
    ) if other else ""
    sites_note = "Each site links to its full report with top URLs, searches, slow URLs, errors and crawlers."
    if len(site_rows) == 1:
        sites_note += (" These logs have one main site, so this overview and the site report show nearly the "
                       "same numbers. The overview also counts any other hosts. Open the site report for URL details.")

    body = (
        overview_sections(net)
        + '<h2 class="section">Sites</h2>'
        + card("Requests by Site", table([("Site", False)] + VEC_HEADERS, site_rows), sites_note)
        + other_card
        + source_sections(net)
        + crawler_sections(net)
        + technical_sections(net, [
            ("Days processed", meta["days_processed"]),
            ("Log files", f"{meta['files_processed']:,}"),
            ("Sites with reports", len(report["sites"])),
            ("Logged-in requests", f"{net['logged_in']:,}"),
        ])
    )
    subtitle = (f"All hosts &nbsp;·&nbsp; {total:,} requests &nbsp;·&nbsp; {meta['days_processed']} days "
                f"&nbsp;·&nbsp; Generated {generated(meta)}")
    return page(f"WPVIP Edge Log Report &mdash; {esc(period)}", subtitle, body, chart_data(net, period))


def build_site_html(report: dict, host: str) -> str:
    meta   = report["meta"]
    period = meta["period"]
    s      = report["sites"][host]
    total  = s["totals"][REQUESTS]

    url_cards = []
    for c in CATEGORIES:
        rows  = s["urls"].get(c, [])
        first = "Search query" if c == "search" else "URL"
        trs   = [f"<tr>{td(esc(k), cls='mono')}{vec_cells(v, total)}</tr>" for k, v in rows]
        url_cards.append(card(f"Top {CATEGORY_LABELS[c]} ({len(rows)})",
                              table([(first, False)] + VEC_HEADERS, trs, search=True), esc(CATEGORY_NOTES[c])))

    slow_rows = [
        f"<tr>{td(CATEGORY_LABELS.get(c, esc(c)))}{td(esc(k), cls='mono')}{td(f'{v[REQUESTS]:,}', v[REQUESTS], 'num')}"
        f"{td(fmt_secs(v[TIME]), v[TIME], 'num')}{td(fmt_ms(v), ratio(v[TIME], v[REQUESTS]), 'num')}"
        f"{td('—' if cache_rate(v) is None else f'{cache_rate(v) * 100:.1f}%', cache_rate(v) or 0, 'num')}</tr>"
        for c, k, v in s["slow"]
    ]
    slow_headers = [("Category", False), ("URL", False), ("Requests", True), ("Total time", True),
                    ("Avg time", True), ("Cache hit", True)]

    body = (
        overview_sections(s)
        + '<h2 class="section">Top URLs by Category</h2>'
        + "".join(url_cards)
        + '<h2 class="section">Performance and Errors</h2>'
        + card(f"Slowest URLs by Total Time ({len(slow_rows)})", table(slow_headers, slow_rows, search=True),
               "Total time = requests × response time. High totals with a low cache hit rate cost the origin most. "
               "Assets are left out because their time is mostly download time.")
        + '<div class="grid-2">'
        + card("404 Not Found", count_table(s["tops"]["errors_404"], total, "URL", mono=True),
               "URLs that returned 404. Look for broken internal links, old URLs and scanner probes. "
               "Searches show the search query instead of a URL.")
        + card("5xx Server Errors", count_table(s["tops"]["errors_5xx"], total, "URL", mono=True),
               "URLs that returned a server error, status 500 to 599. These point to failing code or "
               "timeouts at the origin.")
        + "</div>"
        + source_sections(s)
        + crawler_sections(s)
        + technical_sections(s, [
            ("Share of all requests", pct(total, report["network"]["totals"][REQUESTS], 2)),
            ("Days processed", meta["days_processed"]),
            ("Logged-in requests", f"{s['logged_in']:,}"),
        ])
    )
    subtitle = (f'<a href="../report_{esc(period)}.html">&larr; Overview</a> &nbsp;·&nbsp; '
                f"{total:,} requests &nbsp;·&nbsp; Generated {generated(meta)}")
    return page(f"{esc(host)} &mdash; {esc(period)}", subtitle, body, chart_data(s, period))


def write_html(report: dict, output_dir: Path) -> None:
    period = report["meta"]["period"]

    path = output_dir / f"report_{period}.html"
    path.write_text(build_network_html(report), encoding="utf-8")
    print(f"✓ HTML  → {path} ({path.stat().st_size / 1024:.0f} KB)")

    sites_dir = output_dir / "sites"
    sites_dir.mkdir(parents=True, exist_ok=True)
    for host in report["sites"]:
        path = sites_dir / site_filename(period, host)
        path.write_text(build_site_html(report, host), encoding="utf-8")
        print(f"✓ HTML  → sites/{path.name} ({path.stat().st_size / 1024:.0f} KB)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="WPVIP Edge Log Report Generator")
    parser.add_argument("log_dir",  help="Month folder in the S3 layout, e.g. ~/wpvip-logs/http_requests_edge/2026/09/")
    parser.add_argument("--output", help="Output directory for report files (default: reports/<YYYY-MM> next to http_requests_edge)", default=None)
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of days to process in parallel (default: 1). Each worker can use about 3 GB "
                             "of RAM on a busy site, so use 3 for 16 GB and 6 for 32 GB.")
    parser.add_argument("--force", action="store_true", help="Reprocess days that already have a daily report")
    parser.add_argument("--html-only", action="store_true", help="Rebuild the HTML reports from the monthly JSON only")
    args = parser.parse_args()

    if args.workers < 1:
        parser.error("--workers must be at least 1")

    log_dir = Path(args.log_dir).expanduser().resolve()
    if len(log_dir.parents) < 3 or not (DAY_DIR.fullmatch(log_dir.name) and YEAR_DIR.fullmatch(log_dir.parent.name)):
        parser.error(f"{log_dir} is not a month folder in the S3 layout <YYYY>/<MM>/, "
                     "e.g. ~/wpvip-logs/http_requests_edge/2026/09/")

    period     = f"{log_dir.parent.name}-{log_dir.name}"
    # <root>/http_requests_edge/<YYYY>/<MM>/ → <root>/reports/<YYYY-MM>/
    output_dir = Path(args.output).expanduser().resolve() if args.output else log_dir.parents[2] / "reports" / period
    daily_dir  = output_dir / "daily"
    json_path  = output_dir / f"report_{period}.json"

    if args.html_only:
        report = read_json(json_path)
        if not report or report["meta"].get("schema") != SCHEMA:
            print(f"Error: {json_path} is missing or from an older script version. Run without --html-only.")
            sys.exit(1)
        write_html(report, output_dir)
        return

    # A missing log folder is fine: the monthly report is rebuilt from existing daily reports.
    day_dirs = sorted(d for d in log_dir.iterdir() if d.is_dir() and DAY_DIR.fullmatch(d.name)) if log_dir.is_dir() else []
    if log_dir.is_dir() and not day_dirs and any(log_dir.glob("*.gz")):
        print(f"Error: {log_dir} has .gz files but no day folders (<DD>/). Sync the whole month folder from S3.")
        sys.exit(1)

    print(f"\n{'─'*60}")
    print(f"  WPVIP Edge Log Report Generator")
    print(f"  Input  : {log_dir}{'' if log_dir.is_dir() else '  (missing — merging existing daily reports)'}")
    print(f"  Output : {output_dir}")
    print(f"  Workers: {args.workers}")
    print(f"{'─'*60}\n")

    # Find days that need processing
    jobs = []
    for d in day_dirs:
        day_period = f"{period}-{d.name}"
        daily_path = daily_dir / f"report_{day_period}.json.gz"
        file_count = sum(1 for _ in d.rglob("*.gz"))
        if not file_count:
            continue
        meta = None if args.force else (read_json(daily_path) or {}).get("meta")
        # Reprocess when the folder gained files (a partial day synced again) or the format changed.
        if meta and meta.get("schema") == SCHEMA and file_count <= meta["files_processed"]:
            print(f"  · {day_period}  up to date ({file_count:,} files)")
            continue
        jobs.append((d, day_period, daily_path))

    # Process days
    failed = []
    if jobs:
        print(f"\n  Processing {len(jobs)} day(s)...\n")

    def done(result):
        day_period, total, files, secs = result
        print(f"  ✓ {day_period}  {total:>12,} requests  {files:>7,} files  {secs/60:5.1f} min")

    if args.workers == 1:
        for job in jobs:
            try:
                done(run_day(*job, show_progress=True))
            except Exception as ex:
                print(f"  ⚠ {job[1]} failed: {ex}")
                failed.append(job[1])
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(run_day, *job): job[1] for job in jobs}
            for future in as_completed(futures):
                try:
                    done(future.result())
                except Exception as ex:
                    print(f"  ⚠ {futures[future]} failed: {ex}")
                    failed.append(futures[future])

    # Merge daily reports into the monthly report
    daily_paths = sorted(daily_dir.glob(f"report_{period}-*.json.gz"))
    if not daily_paths:
        print(f"\nError: no daily reports found in {daily_dir}")
        sys.exit(1)

    print(f"\n  Merging {len(daily_paths)} daily report(s)...")
    report = merge_days(daily_paths, period, log_dir)
    if not report["meta"]["days_processed"]:
        print(f"\nError: no usable daily reports in {daily_dir}")
        sys.exit(1)

    write_json(json_path, report)
    print(f"\n✓ JSON  → {json_path} ({json_path.stat().st_size / 1024:.0f} KB)")
    write_html(report, output_dir)

    total = report["meta"]["total_requests"]
    print(f"\n{'─'*60}")
    print(f"  Period          : {period}")
    print(f"  Days            : {report['meta']['days_processed']}")
    print(f"  Total requests  : {total:,}")
    print()
    for host, s in report["sites"].items():
        v = s["totals"]
        print(f"  {host:<24} {v[REQUESTS]:>14,}  {pct(v[REQUESTS], total):>6}   bot {pct(v[BOT], v[REQUESTS]):>6}")
    print(f"{'─'*60}\n")

    if failed:
        print(f"⚠ Failed days, not in this report: {', '.join(sorted(failed))}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
