#!/usr/bin/env python3
"""
e2recorder.py — Serien-Aufnahme-Scheduler für e2proxy Recording API
====================================================================
Delegiert alle Aufnahmen an /api/record/start auf konfigurierten e2proxy-Instanzen.
Wählt automatisch den Proxy mit den meisten freien Tunern.

Endpoints:
  GET  /                          → Web-UI
  GET  /api/status                → Service-Status
  GET  /api/config                → Konfiguration
  POST /api/config                → Konfiguration speichern
  POST /api/discover              → SSDP-Discovery
  GET  /api/proxies               → Alle Proxies + Tuner-Status
  GET  /api/series                → Alle Serien
  POST /api/series                → Serie hinzufügen
  PUT  /api/series/<id>           → Serie bearbeiten
  DELETE /api/series/<id>         → Serie löschen
  POST /api/series/from-epg       → Serie direkt aus EPG-Klick anlegen
  GET  /api/tmdb/search?q=...     → TMDB Suche
  GET  /api/schedule              → Aufnahmeplan (EPG + Matches)
  POST /api/scan                  → EPG-Scan manuell
  GET  /api/recordings            → Aufnahmen-Liste
  POST /api/recordings/<id>/keep  → Aufnahme schützen
  DELETE /api/recordings/<id>     → Aufnahme löschen
  DELETE /api/schedule/<id>       → Einzelne Aufnahme überspringen
  GET  /api/logs                  → Log-Einträge
  POST /api/cleanup               → Cleanup manuell
"""

import http.server
import urllib.request
import urllib.parse
import threading
import time
import logging
import sys
import os
import re
import json
import signal
import uuid
import collections
from datetime import datetime

# ── Pfade ──────────────────────────────────────────────────────────────────
DATA_DIR        = os.environ.get("E2REC_DATA_DIR", "/data")
CONFIG_FILE     = f"{DATA_DIR}/config.json"

# ── Version ────────────────────────────────────────────────────────────────
VERSION = "1.4.1+94ccd5e4"   # Fix: Senderliste wird nach EPG-Scan und beim Seitenstart aktualisiert
SERIES_FILE     = f"{DATA_DIR}/series.json"
RECORDINGS_FILE = f"{DATA_DIR}/recordings.json"
HISTORY_FILE    = f"{DATA_DIR}/tuner_history.json"

# ── Logging ────────────────────────────────────────────────────────────────
_LOG_BUFFER = collections.deque(maxlen=500)

class _RingHandler(logging.Handler):
    def emit(self, record):
        try:
            _LOG_BUFFER.append({
                "ts":    self.formatter.formatTime(record, "%H:%M:%S"),
                "level": record.levelname,
                "msg":   record.getMessage(),
            })
        except Exception:
            pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("e2recorder")
_rh = _RingHandler()
_rh.setFormatter(logging.Formatter())
_rh.setLevel(logging.DEBUG)
log.addHandler(_rh)


def _setup_file_logging():
    """Tägliche Log-Rotation in DATA_DIR/logs/, konfigurierbare Aufbewahrung."""
    import logging.handlers as _lh
    log_dir = os.path.join(DATA_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "e2recorder.log")
    retention = int(_config.get("log_retention_days", 30))
    fh = _lh.TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=retention,
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    fh.suffix = "%Y-%m-%d.log"
    log.addHandler(fh)
    log.info(f"File-Logging: {log_file} ({retention} Tage Aufbewahrung)")

# ── Config ─────────────────────────────────────────────────────────────────
_config = {}
_config_lock = threading.Lock()

CONFIG_DEFAULTS = {
    # Proxies — Liste von {url, name, enabled}
    # Wird via SSDP oder manuell befüllt
    "proxies": [],
    # Service
    "recorder_port":        8889,
    # Aufnahmen
    # Leer = e2proxy verwendet seinen eigenen konfigurierten Pfad (empfohlen)
    # Gesetzt = wird als path-Parameter an /api/record/start übergeben
    "recordings_subdir":    "",
    "stream_profile":       "remux-ac3",
    "pre_buffer_sec":       30,
    "post_buffer_sec":      60,
    # Cleanup
    "cleanup_trigger":      "on_new",
    "cleanup_hour":         4,
    # EPG
    "epg_scan_interval":    3600,
    "epg_lookahead_hours":  72,
    # TMDB
    "tmdb_api_key":         "",
    "tmdb_language":        "de-DE",
    "log_level":            "INFO",
    "api_call_logging":     False,
    "log_retention_days":   30,
}

def load_config():
    global _config
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                _config = {**CONFIG_DEFAULTS, **json.load(f)}
            log.info(f"Config geladen: {CONFIG_FILE}")
            _apply_log_level()
            return
        except Exception as e:
            log.warning(f"Config laden: {e}")
    _config = dict(CONFIG_DEFAULTS)
    _save_config_locked()
    _apply_log_level()

# Max. 200 Einträge in der History
HISTORY_MAX = 200

def log_tuner_event(rec_title, proxy_statuses, chosen_url, error=None):
    """Loggt einen Proxy/Tuner Check-Event für spätere Analyse."""
    try:
        entry = {
            "ts":       datetime.now().isoformat(),
            "title":    rec_title,
            "proxies":  proxy_statuses,  # [{url, free, total, online}]
            "chosen":   chosen_url,
            "error":    error,
        }
        try:
            with open(HISTORY_FILE) as f:
                history = json.load(f)
        except Exception:
            history = []
        history.append(entry)
        # Nur die letzten N behalten
        if len(history) > HISTORY_MAX:
            history = history[-HISTORY_MAX:]
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, ensure_ascii=False, default=str)
    except Exception as e:
        log.debug(f"log_tuner_event Fehler: {e}")


def _apply_log_level():
    """Setzt den Log-Level aus der Config."""
    level_str = _config.get("log_level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logging.getLogger().setLevel(level)
    log.setLevel(level)
    log.debug(f"Log-Level gesetzt: {level_str}")


def _save_config_locked():
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(_config, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.error(f"Config speichern: {e}")

def save_config():
    with _config_lock:
        _save_config_locked()

def cfg(key):
    with _config_lock:
        return _config.get(key, CONFIG_DEFAULTS.get(key))

def update_config(new_vals):
    global _config
    with _config_lock:
        for k, v in new_vals.items():
            # Rückwärtskompatibilität: recordings_dir → recordings_subdir
            if k == "recordings_dir":
                k = "recordings_subdir"
            if k in CONFIG_DEFAULTS:
                _config[k] = v
        _save_config_locked()

def get_config_dict():
    with _config_lock:
        return dict(_config)

# ── Proxy Management ───────────────────────────────────────────────────────
_proxy_lock = threading.Lock()

def get_proxies():
    """Gibt alle konfigurierten Proxies zurück."""
    with _config_lock:
        return list(_config.get("proxies", []))

def add_or_update_proxy(url, name="", enabled=True):
    """Fügt einen Proxy hinzu oder aktualisiert ihn."""
    url = url.rstrip("/")
    with _config_lock:
        proxies = _config.get("proxies", [])
        for p in proxies:
            if p["url"] == url:
                p["name"]    = name or p.get("name", url)
                p["enabled"] = enabled
                _save_config_locked()
                return p
        entry = {"url": url, "name": name or url, "enabled": enabled}
        proxies.append(entry)
        _config["proxies"] = proxies
        _save_config_locked()
        return entry

def remove_proxy(url):
    with _config_lock:
        proxies = [p for p in _config.get("proxies", []) if p["url"] != url]
        _config["proxies"] = proxies
        _save_config_locked()

def fetch_proxy_status(proxy_url):
    """
    Holt Tuner-Status via /api/health (schnell, kein EPG-Overhead).
    Fallback auf /api/tuners für ältere e2proxy Versionen.
    """
    data, err = _api_call(f"{proxy_url}/api/health", timeout=4)
    if not err and data and data.get("ok"):
        # /api/health Format → in /api/tuners Format übersetzen
        return {
            "free":      data.get("tuners_free", 0),
            "total":     len(data.get("receivers", [])),
            "receivers": [
                {
                    "id":        r.get("id"),
                    "name":      r.get("name"),
                    "busy":      r.get("busy", False),
                    "channel":   r.get("channel", ""),
                    "client_ip": "",
                    "since":     "",
                }
                for r in data.get("receivers", [])
            ],
        }
    # Fallback: /api/tuners (ältere e2proxy)
    data2, err2 = _api_call(f"{proxy_url}/api/tuners", timeout=4)
    return data2 if not err2 else None

def get_proxy_url():
    """Gibt die URL des ersten aktiven Proxys zurück."""
    for p in get_proxies():
        if p.get("enabled", True):
            return p["url"]
    return None


def get_best_proxy(required_ref=None):
    """
    Wählt den besten verfügbaren Proxy.
    Gibt (proxy_url, tuner_status) oder (None, None) zurück.
    Loggt jeden Check in tuner_history.json.
    """
    proxies = [p for p in get_proxies() if p.get("enabled", True)]
    if not proxies:
        log.warning("Keine Proxies konfiguriert")
        return None, None

    best_url    = None
    best_free   = 0
    best_status = None
    proxy_statuses = []

    for p in proxies:
        status = fetch_proxy_status(p["url"])
        if not status:
            proxy_statuses.append({"url": p["url"], "online": False, "free": 0, "total": 0})
            log.debug(f"Proxy nicht erreichbar: {p['url']}")
            continue
        free  = status.get("free", 0)
        total = status.get("total", 0)
        proxy_statuses.append({"url": p["url"], "online": True, "free": free, "total": total,
                                "receivers": status.get("receivers", [])})
        if free > best_free:
            best_free   = free
            best_url    = p["url"]
            best_status = status

    if not best_url:
        log.warning(f"Kein freier Proxy. Status: {proxy_statuses}")
        return None, None

    log.debug(f"Bester Proxy: {best_url} ({best_free} freie Tuner)")
    return best_url, best_status

def get_all_proxy_statuses():
    """Holt Status aller Proxies für die UI."""
    result = []
    for p in get_proxies():
        status = fetch_proxy_status(p["url"]) if p.get("enabled", True) else None
        result.append({
            "url":      p["url"],
            "name":     p.get("name", p["url"]),
            "enabled":  p.get("enabled", True),
            "online":   status is not None,
            "total":    status.get("total", 0) if status else 0,
            "busy":     status.get("busy", 0)  if status else 0,
            "free":     status.get("free", 0)  if status else 0,
            "receivers": status.get("receivers", []) if status else [],
        })
    return result

# ── SSDP Discovery ─────────────────────────────────────────────────────────
import socket

SSDP_MCAST = "239.255.255.250"
SSDP_PORT  = 1900
_discovery_status = {"state": "idle", "last_run": None, "found": []}

def run_discovery(manual=False):
    """SSDP-Discovery — findet alle e2proxy Instanzen im Netzwerk."""
    _discovery_status["state"]    = "running"
    _discovery_status["last_run"] = datetime.now().isoformat()
    found = []

    msg = (
        "M-SEARCH * HTTP/1.1\r\n"
        f"HOST: {SSDP_MCAST}:{SSDP_PORT}\r\n"
        'MAN: "ssdp:discover"\r\n'
        "MX: 3\r\n"
        "ST: upnp:rootdevice\r\n"
        "\r\n"
    ).encode()

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        sock.settimeout(5)
        sock.sendto(msg, (SSDP_MCAST, SSDP_PORT))

        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                data, addr = sock.recvfrom(4096)
                resp = data.decode("utf-8", errors="replace")
                if "e2proxy" not in resp.lower():
                    continue
                m = re.search(r"LOCATION:\s*(http://[^\s/]+)", resp, re.I)
                if m:
                    url = m.group(1).rstrip("/")
                    if url not in found:
                        found.append(url)
                        log.info(f"SSDP: e2proxy gefunden → {url}")
                        add_or_update_proxy(url, name=f"e2proxy @ {addr[0]}")
            except socket.timeout:
                break
            except Exception:
                pass
        sock.close()
    except Exception as e:
        log.warning(f"SSDP Fehler: {e}")

    _discovery_status["state"] = "ok" if found else "not_found"
    _discovery_status["found"] = found
    log.info(f"SSDP-Discovery abgeschlossen: {len(found)} Proxy(s) gefunden")
    return found

def discovery_scheduler():
    """Wiederholt Discovery wenn keine Proxies online."""
    time.sleep(10)
    run_discovery()
    while True:
        time.sleep(120)
        proxies = get_proxies()
        if not proxies:
            run_discovery()

# ── EPG Fetch ──────────────────────────────────────────────────────────────
_epg_cache      = []
_epg_cache_ts   = 0.0
_epg_cache_lock = threading.Lock()

def _api_call(url, method="GET", body=None, timeout=10):
    """
    Zentraler API-Call mit optionalem Logging.
    Gibt (response_dict, error_str) zurück.
    """
    do_log = cfg("api_call_logging")
    if do_log:
        log.debug(f"API → {method} {url}" + (f" body={body}" if body else ""))
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/json"} if body else {},
            method=method)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read())
        if do_log:
            log.debug(f"API ← {url} → {str(data)[:200]}")
        return data, None
    except Exception as e:
        if do_log:
            log.debug(f"API ✗ {url} → {e}")
        else:
            log.debug(f"API-Call fehlgeschlagen: {url}: {e}")
        return None, str(e)


def fetch_epg_from_proxy(proxy_url):
    """Holt EPG von einem bestimmten Proxy."""
    data, err = _api_call(f"{proxy_url}/api/epg/data", timeout=15)
    if err:
        log.error(f"EPG fetch von {proxy_url}: {err}")
        return []
    if data and data.get("ok"):
        return data.get("channels", [])
    return []

def fetch_epg():
    """Holt EPG vom ersten verfügbaren Proxy."""
    for p in get_proxies():
        if not p.get("enabled", True):
            continue
        channels = fetch_epg_from_proxy(p["url"])
        if channels:
            return channels
    return []

def get_cached_epg():
    global _epg_cache, _epg_cache_ts
    with _epg_cache_lock:
        if time.time() - _epg_cache_ts < 300 and _epg_cache:
            return list(_epg_cache)
    data = fetch_epg()
    with _epg_cache_lock:
        _epg_cache    = data
        _epg_cache_ts = time.time()
    return data

def fetch_channels():
    """Holt Favoriten vom ersten verfügbaren Proxy."""
    for p in get_proxies():
        if not p.get("enabled", True):
            continue
        try:
            with urllib.request.urlopen(f"{p['url']}/api/favorites", timeout=10) as r:
                data = json.loads(r.read())
            items = data if isinstance(data, list) else []
            if items:
                return [{"ref": c.get("ref",""), "name": c.get("name",""),
                         "group": c.get("group","")} for c in items if c.get("ref")]
        except Exception as e:
            log.debug(f"Channels fetch: {e}")
    return []

# ── Ref Format ─────────────────────────────────────────────────────────────

def refs_match(fav_ref, epg_id):
    """
    Favoriten-ref:  '1:0:19:EF11:421:1:C00000:0:0:0:' (Doppelpunkte)
    EPG channel id: '1_0_19_EF11_421_1_C00000_0_0_0_' (Underscores)
    e2proxy macht intern: ref.rstrip('/').replace(':', '_')
    Die API liefert refs aber schon mit Underscores in /api/favorites!
    """
    # Beide normalisieren
    def norm(s):
        return s.rstrip("/").rstrip(":").rstrip("_").replace(":", "_")
    return norm(fav_ref) == norm(epg_id) or fav_ref == epg_id

# ── Series DB ──────────────────────────────────────────────────────────────
_series_lock = threading.Lock()

def _load_series():
    try:
        if os.path.exists(SERIES_FILE):
            with open(SERIES_FILE) as f:
                return json.load(f)
    except Exception as e:
        log.warning(f"Serien laden: {e}")
    return []

def _save_series(data):
    try:
        with open(SERIES_FILE, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.error(f"Serien speichern: {e}")

def get_series():
    with _series_lock:
        return _load_series()

def get_serie_by_id(sid):
    return next((s for s in get_series() if s["id"] == sid), None)

def add_serie(name, channel_name, channel_ref,
              keep_last=0, enabled=True,
              regex_pattern="", tmdb_id=None, tmdb_poster="",
              once=False, once_start_ts=None,
              kind=None, year=None,
              pre_offset_sec=0, post_offset_sec=0):
    """
    once=True: Einmalige Aufnahme — Serie wird nach der Aufnahme automatisch deaktiviert.
    once_start_ts: Exakter Startzeitpunkt für einmalige Aufnahme (verhindert andere Folgen).
    """
    with _series_lock:
        data = _load_series()
        # Duplikat-Prüfung: gleicher Name + gleicher Sender
        for existing in data:
            if (existing.get("channel_ref") == channel_ref and
                    existing.get("name", "").lower() == name.lower()):
                log.warning(f"Serie bereits vorhanden: '{name}' auf '{channel_name}' — übersprungen")
                return existing
            if (existing.get("channel_ref") == channel_ref and
                    regex_pattern and
                    existing.get("regex_pattern", "") == regex_pattern):
                log.warning(f"Regex bereits vorhanden: '{regex_pattern}' auf '{channel_name}' — übersprungen")
                return existing
        entry = {
            "id":               str(uuid.uuid4())[:8],
            "name":             name,
            "channel_name":     channel_name,
            "channel_ref":      channel_ref,
            "keep_last":        int(keep_last),
            "enabled":          bool(enabled),
            "regex_pattern":    regex_pattern or re.escape(name),
            "tmdb_id":          tmdb_id,
            "tmdb_poster":      tmdb_poster,
            "once":             bool(once),
            "once_start_ts":    once_start_ts,
            "kind":             kind or ("movie" if once else "series"),  # movie/series
            "year":             year,
            "pre_offset_sec":   int(pre_offset_sec),   # Aufnahme früher starten
            "post_offset_sec":  int(post_offset_sec),  # Aufnahme länger laufen lassen
            "created":          datetime.now().isoformat(),
        }
        data.append(entry)
        _save_series(data)
        log.info(f"Serie hinzugefügt: {name} / regex: {entry['regex_pattern']}")
        return entry

def update_serie(sid, updates):
    with _series_lock:
        data = _load_series()
        for i, s in enumerate(data):
            if s["id"] == sid:
                if "regex_pattern" in updates and not updates["regex_pattern"].strip():
                    updates["regex_pattern"] = re.escape(updates.get("name", s["name"]))
                data[i] = {**s, **updates}
                _save_series(data)
                return data[i]
    return None

def delete_serie(sid):
    with _series_lock:
        data = _load_series()
        new = [s for s in data if s["id"] != sid]
        _save_series(new)
        return len(new) < len(data)

# ── Recordings DB ──────────────────────────────────────────────────────────
_rec_lock = threading.Lock()

def _load_recs():
    try:
        if os.path.exists(RECORDINGS_FILE):
            with open(RECORDINGS_FILE) as f:
                return json.load(f)
    except Exception as e:
        log.warning(f"Aufnahmen laden: {e}")
    return []

def _save_recs(data):
    try:
        with open(RECORDINGS_FILE, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.error(f"Aufnahmen speichern: {e}")

def get_recordings():
    with _rec_lock:
        return _load_recs()

def get_rec_by_id(rid):
    return next((r for r in get_recordings() if r["id"] == rid), None)

def get_rec_by_id_key(channel_ref, start_ts):
    """Sucht Aufnahme nach channel_ref + start_ts (event_key)."""
    key = _event_key(channel_ref, int(start_ts))
    return next(
        (r for r in get_recordings()
         if _event_key(r.get("channel_ref",""), r.get("start_ts",0)) == key),
        None
    )

def upsert_rec(rec):
    with _rec_lock:
        data = _load_recs()
        for i, r in enumerate(data):
            if r["id"] == rec["id"]:
                data[i] = rec
                _save_recs(data)
                return
        data.append(rec)
        _save_recs(data)

def delete_rec_entry(rid):
    with _rec_lock:
        data = _load_recs()
        _save_recs([r for r in data if r["id"] != rid])

# ── Title Matching ─────────────────────────────────────────────────────────

def compile_pattern(serie):
    pat = serie.get("regex_pattern", "") or re.escape(serie["name"])
    try:
        return re.compile(pat, re.IGNORECASE)
    except re.error:
        try:
            return re.compile(re.escape(serie["name"]), re.IGNORECASE)
        except Exception:
            return None

def title_matches(epg_title, serie):
    rx = compile_pattern(serie)
    return bool(rx and rx.search(epg_title))

# ── TMDB ───────────────────────────────────────────────────────────────────

def tmdb_search(query):
    api_key  = cfg("tmdb_api_key")
    language = cfg("tmdb_language")
    if not api_key:
        return {"error": "Kein TMDB API Key konfiguriert"}
    results = []
    for kind in ("tv", "movie"):
        url = (f"https://api.themoviedb.org/3/search/{kind}"
               f"?api_key={api_key}&query={urllib.parse.quote(query)}&language={language}")
        try:
            with urllib.request.urlopen(url, timeout=8) as r:
                data = json.loads(r.read())
            for item in data.get("results", [])[:4]:
                tmdb_id  = item.get("id")
                name     = item.get("name") or item.get("title", "")
                orig     = item.get("original_name") or item.get("original_title", "")
                year     = (item.get("first_air_date") or item.get("release_date") or "")[:4]
                poster   = item.get("poster_path")
                aliases  = _tmdb_get_de_titles(tmdb_id, kind, api_key, language)
                all_titles = list({name, orig} | set(aliases) - {""})
                results.append({
                    "tmdb_id":          tmdb_id,
                    "kind":             kind,
                    "name":             name,
                    "original_name":    orig,
                    "year":             year,
                    "poster":           f"https://image.tmdb.org/t/p/w92{poster}" if poster else "",
                    "poster_large":     f"https://image.tmdb.org/t/p/w300{poster}" if poster else "",
                    "aliases":          aliases,
                    "regex_suggestion": _build_regex(all_titles),
                })
        except Exception as e:
            log.warning(f"TMDB search ({kind}): {e}")
    return {"results": results[:6]}

def _tmdb_get_de_titles(tmdb_id, kind, api_key, language):
    titles = set()
    try:
        url = f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/translations?api_key={api_key}"
        with urllib.request.urlopen(url, timeout=6) as r:
            data = json.loads(r.read())
        for t in data.get("translations", []):
            if t.get("iso_639_1") == "de":
                td = t.get("data", {})
                n = td.get("name") or td.get("title") or ""
                if n:
                    titles.add(n)
    except Exception:
        pass
    try:
        url = f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/alternative_titles?api_key={api_key}"
        with urllib.request.urlopen(url, timeout=6) as r:
            data = json.loads(r.read())
        key = "results" if kind == "tv" else "titles"
        for t in data.get(key, []):
            if t.get("iso_3166_1") in ("DE", "AT", "CH"):
                n = t.get("title") or t.get("name") or ""
                if n:
                    titles.add(n)
    except Exception:
        pass
    return list(titles)

def _build_regex(titles):
    if not titles:
        return ""
    parts = []
    seen = set()
    for t in sorted(titles, key=len, reverse=True):
        norm = re.sub(r"['\u2018\u2019\u201c\u201d]", ".", t)
        norm = re.sub(r"\s+", r"\\s+", norm.strip())
        if norm.lower() not in seen:
            seen.add(norm.lower())
            parts.append(norm)
    unique = []
    for p in parts:
        if not any(other != p and other.startswith(p) for other in parts):
            unique.append(p)
    return "|".join(unique or parts)

# ── Scheduling ─────────────────────────────────────────────────────────────
_scheduled      = {}
_scheduled_lock = threading.Lock()
_scan_status    = {"running": False, "last_run": None, "found": 0, "scheduled": 0}
_last_scan      = 0.0

def _event_key(channel_ref, start_ts):
    # Ref normalisieren, damit Doppelpunkt- und Underscore-Format denselben Key
    # ergeben (Serie speichert oft '1:0:...', EPG liefert '1_0_...').
    ref = (channel_ref or "").rstrip("/").rstrip(":").rstrip("_").replace(":", "_")
    return f"{ref}@{int(start_ts)}"

def _safe_filename(title, start_ts):
    dt = datetime.fromtimestamp(start_ts)
    date_str = dt.strftime("%Y-%m-%d_%H-%M")
    safe = re.sub(r'[^\w\s\-äöüÄÖÜß]', '', title)
    safe = re.sub(r'\s+', '_', safe.strip())[:60]
    return f"{safe}_{date_str}.ts"

def _output_path(serie, title, start_ts):
    base = cfg("recordings_subdir") or "/tmp/e2recorder"
    serie_dir = re.sub(r'[^\w\s\-äöüÄÖÜß]', '', serie["name"])
    serie_dir = re.sub(r'\s+', '_', serie_dir.strip())
    out_dir = os.path.join(base, serie_dir)
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, _safe_filename(title, start_ts))

def run_epg_scan(manual=False):
    global _last_scan
    if _scan_status["running"]:
        return
    _scan_status["running"]  = True
    _scan_status["last_run"] = datetime.now().isoformat()
    try:
        log.info(f"EPG-Scan gestartet (manual={manual})")
        epg_channels = fetch_epg()
        global _epg_cache, _epg_cache_ts
        with _epg_cache_lock:
            _epg_cache    = epg_channels
            _epg_cache_ts = time.time()

        series_list = [s for s in get_series() if s.get("enabled", True)]
        if not epg_channels:
            log.warning("EPG leer")
            return

        now_ts    = time.time()
        lookahead = cfg("epg_lookahead_hours") * 3600
        pre       = cfg("pre_buffer_sec")
        found = scheduled = 0

        for serie in series_list:
            ch_data = next(
                (ch for ch in epg_channels
                 if refs_match(serie["channel_ref"], ch["id"]) or ch["name"] == serie["channel_name"]),
                None
            )
            if not ch_data:
                continue

            for ev in ch_data.get("events", []):
                start_ts = ev.get("start", 0)
                stop_ts  = ev.get("stop", 0)
                title    = ev.get("title", "")

                if start_ts < now_ts - pre:
                    continue
                if start_ts > now_ts + lookahead:
                    continue
                if not title_matches(title, serie):
                    continue

                # Einmalige Aufnahme: nur den exakten Sendetermin aufnehmen
                if serie.get("once") and serie.get("once_start_ts"):
                    if int(start_ts) != int(serie["once_start_ts"]):
                        continue  # Andere Folgen überspringen

                found += 1
                key = _event_key(serie["channel_ref"], start_ts)
                with _scheduled_lock:
                    if key in _scheduled:
                        continue
                # Auch DB prüfen (Schutz gegen Duplikate nach Restart)
                existing = get_rec_by_id_key(serie["channel_ref"], start_ts)
                if existing and existing.get("status") not in ("failed", "skipped"):
                    # In _scheduled eintragen damit zukünftige Scans es erkennen
                    with _scheduled_lock:
                        _scheduled[key] = existing
                    continue

                fp  = _output_path(serie, title, start_ts)
                # Pre/Post-Offset aus Serie anwenden (kann früher/später starten/enden)
                pre_off  = int(serie.get("pre_offset_sec", 0))
                post_off = int(serie.get("post_offset_sec", 0))
                rec = {
                    "id":           str(uuid.uuid4())[:8],
                    "serie_id":     serie["id"],
                    "serie_name":   serie["name"],
                    "title":        title,
                    "subtitle":     ev.get("sub", ""),
                    "desc":         ev.get("desc", ""),
                    "channel_name": serie["channel_name"],
                    "channel_ref":  serie["channel_ref"],
                    "start_ts":     start_ts - pre_off,    # früher starten (pre_off >0)
                    "stop_ts":      stop_ts + post_off,    # länger laufen (post_off >0)
                    "epg_start_ts": start_ts,              # Original EPG-Zeiten
                    "epg_stop_ts":  stop_ts,
                    "filepath":     fp,
                    "status":       "scheduled",
                    "protected":    False,
                    "source":       "epg-scheduler",
                    "proxy_url":    None,
                    "proxy_rec_id": None,
                    "tmdb_poster":  serie.get("tmdb_poster", ""),
                    "created_at":   datetime.now().isoformat(),
                }
                with _scheduled_lock:
                    _scheduled[key] = rec
                upsert_rec(rec)
                scheduled += 1
                log.info(f"Eingeplant: '{title}' ({serie['channel_name']}) "
                         f"am {datetime.fromtimestamp(start_ts).strftime('%d.%m. %H:%M')}")

        _scan_status["found"]     = found
        _scan_status["scheduled"] = scheduled
        _last_scan = time.time()
        log.info(f"EPG-Scan fertig: {found} Treffer, {scheduled} neu geplant")
    except Exception as e:
        log.error(f"EPG-Scan Fehler: {e}")
    finally:
        _scan_status["running"] = False

def epg_scan_scheduler():
    time.sleep(5)
    run_epg_scan()
    while True:
        time.sleep(cfg("epg_scan_interval"))
        run_epg_scan()

# ── Recording via e2proxy API ──────────────────────────────────────────────

def _proxy_recording_running(proxy_url, proxy_rec_id):
    """
    Prüft via /api/record/status ob eine Aufnahme am Proxy noch läuft.
    Gibt (running, remaining_sec, rec_data) zurück.
    running=None bedeutet Fehler beim Status-Check.
    """
    try:
        with urllib.request.urlopen(f"{proxy_url}/api/record/status", timeout=5) as r:
            data = json.loads(r.read())
        for entry in data.get("recordings", []):
            if entry.get("id") == proxy_rec_id:
                running = entry.get("running", False)
                return running, entry.get("remaining_sec", 0), entry
        # Nicht in der Liste → Aufnahme beendet (Proxy-Watchdog hat gestoppt)
        return False, 0, None
    except Exception as e:
        log.debug(f"Status-Check {proxy_url}: {e}")
        return None, 0, None  # None = Fehler, nicht sicher

def _stop_proxy_recording(proxy_url, proxy_rec_id):
    """Sendet Stop-Request an den Proxy. Gibt (ok, filepath) zurück."""
    body = json.dumps({"recording_id": proxy_rec_id}).encode()
    resp, err = _api_call(f"{proxy_url}/api/record/stop",
                          method="POST", body=body, timeout=10)
    if err or not resp:
        log.warning(f"Stop fehlgeschlagen: {err}")
        return False, ""
    filepath = resp.get("file", "")
    log.info(f"Aufnahme gestoppt: {filepath}")
    return resp.get("ok", False), filepath

def do_record(rec):
    """
    Startet Aufnahme via e2proxy Recording API.
    Watchdog überwacht via /api/record/status ob die Aufnahme noch läuft.
    """
    duration = (rec["stop_ts"] - rec["start_ts"]) + cfg("post_buffer_sec")
    pre      = cfg("pre_buffer_sec")

    # Auf Sendestart warten
    wait = rec["start_ts"] - pre - time.time()
    if wait > 0:
        log.debug(f"Warte {wait:.0f}s bis '{rec['title']}'")
        time.sleep(wait)

    # ── Intelligentes Tuner-Management ──────────────────────────────────
    # Strategie:
    # 1. Prüfe ob Tuner frei ist
    # 2. Wenn belegt: frage /api/record/status wie lange laufende Aufnahmen noch dauern
    # 3. Wenn eine Aufnahme bald endet (< max_wait_sec): warte aktiv
    # 4. 20s Toleranz nach geplantem Start → noch okay
    # 5. Wenn zu lange warten nötig: fail mit detaillierter Begründung

    log.debug(f"Suche freien Proxy für '{rec['title']}'")

    max_late_sec  = 60   # max. 60s nach geplantem Start noch starten (Pre-Buffer deckts ab)
    poll_interval = 10   # alle 10s neu prüfen
    waited_sec    = 0

    while True:
        proxy_url, tuner_status = get_best_proxy()

        # Tuner frei → sofort starten
        if proxy_url and tuner_status and tuner_status.get("free", 0) > 0:
            if waited_sec > 0:
                log.info(f"Tuner frei nach {waited_sec}s Warten — starte '{rec['title']}'")
            break

        # Kein Proxy erreichbar
        if not proxy_url:
            error_msg = "Kein Proxy erreichbar"
            rec["status"] = "failed"
            rec["error"]  = error_msg
            log.error(f"'{rec['title']}': {error_msg}")
            upsert_rec(rec)
            return

        # Alle Tuner belegt — analysieren warum
        time_since_start = max(0, time.time() - rec["start_ts"])
        best_remaining   = None
        busy_by_recorder = False  # Tuner durch eigene Aufnahme belegt
        busy_by_client   = False  # Tuner durch Plex/externen Client belegt

        best_proxy_url = proxy_url or (get_proxies()[0]["url"] if get_proxies() else None)
        if best_proxy_url:
            # Prüfe laufende Recorder-Aufnahmen → remaining_sec
            status_data, _ = _api_call(f"{best_proxy_url}/api/record/status", timeout=5)
            if status_data:
                running = [r for r in status_data.get("recordings", []) if r.get("running")]
                if running:
                    best_remaining   = min(r.get("remaining_sec", 9999) for r in running)
                    busy_by_recorder = True

            # Prüfe ob Tuner durch externen Client (Plex etc.) belegt
            receivers = (tuner_status or {}).get("receivers", [])
            busy_receivers = [r for r in receivers if r.get("busy")]
            if busy_receivers and not busy_by_recorder:
                busy_by_client = True
                # Externer Client: wir schätzen wie lange er noch streamt
                # Konservativ: 10 Minuten Wartezeit maximum
                log.info(
                    f"'{rec['title']}': Tuner durch externen Client belegt "
                    f"({', '.join(r['channel'] for r in busy_receivers)}) — "
                    f"warte auf Freigabe ({time_since_start:.0f}s seit Start)"
                )
            elif busy_by_recorder:
                log.info(
                    f"'{rec['title']}': Tuner durch Aufnahme belegt, "
                    f"endet in {best_remaining:.0f}s "
                    f"({time_since_start:.0f}s seit Sendestart)"
                )

        # Entscheidung: warten oder aufgeben?
        if time_since_start > max_late_sec:
            # Zu spät — würde zu viel verpassen
            receivers = (tuner_status or {}).get("receivers", [])
            busy_info = "; ".join(
                f"{r['name']}: {r.get('channel','?')}"
                + (f" (noch ~{best_remaining:.0f}s)" if best_remaining else "")
                for r in receivers if r.get("busy")
            )
            if busy_by_client:
                error_msg = f"Kein freier Tuner nach {time_since_start:.0f}s — externer Client aktiv: {busy_info}"
            else:
                error_msg = f"Zu spät gestartet ({time_since_start:.0f}s) — {busy_info}"
            proxy_statuses = [{"url": (best_proxy_url or "?"), "online": True,
                               "free": 0,
                               "total": (tuner_status or {}).get("total", 0),
                               "receivers": (tuner_status or {}).get("receivers", [])}]
            log_tuner_event(rec["title"], proxy_statuses, None, error=error_msg)
            rec["status"] = "failed"
            rec["error"]  = error_msg
            log.error(f"'{rec['title']}': {error_msg}")
            upsert_rec(rec)
            return

        if busy_by_recorder and best_remaining is not None:
            # Eigene Aufnahme läuft — prüfe ob Warten sich lohnt
            time_left = max_late_sec - time_since_start
            if best_remaining + 5 > time_left:
                error_msg = (
                    f"Aufnahme endet in {best_remaining:.0f}s, "
                    f"aber nur noch {time_left:.0f}s Toleranz — übersprungen"
                )
                rec["status"] = "skipped"
                rec["error"]  = error_msg
                log.warning(f"'{rec['title']}': {error_msg}")
                upsert_rec(rec)
                return
            # Sonst: warten lohnt sich

        # Warten und nochmal versuchen
        time.sleep(poll_interval)
        waited_sec += poll_interval

    # Erfolgreicher Proxy-Check in History loggen
    log.debug(f"Proxy gewählt: {proxy_url} ({tuner_status.get('free',0)} freie Tuner)")
    log_tuner_event(rec["title"],
                    [{"url": proxy_url, "online": True,
                      "free": tuner_status.get("free",0),
                      "total": tuner_status.get("total",0),
                      "receivers": tuner_status.get("receivers",[])}],
                    proxy_url)

    description = rec.get("subtitle") or rec.get("desc") or ""
    tmdb_poster = rec.get("tmdb_poster", "")

    log.info(f"Starte Aufnahme via {proxy_url}: '{rec['title']}' ({int(duration)}s)")
    rec["status"]      = "recording"
    rec["started_at"]  = datetime.now().isoformat()
    rec["proxy_url"]   = proxy_url
    rec["stop_ts_eff"] = rec["stop_ts"] + cfg("post_buffer_sec")
    upsert_rec(rec)

    try:
        # Ziel-Pfad: e2proxy v3.2 entscheidet selbst via kind
        serie = get_serie_by_id(rec.get("serie_id", ""))
        kind  = (serie or {}).get("kind") or "series"  # default: series
        api_body = {
            "ref":           rec["channel_ref"],
            "title":         rec.get("serie_name") or rec["title"],
            "episode_title": rec.get("subtitle", ""),
            "description":   description,
            "image_url":     tmdb_poster,
            "duration":      int(duration),
            "profile":       cfg("stream_profile"),
            "kind":          kind,
        }
        # Year für Filme
        if (serie or {}).get("year"):
            api_body["year"] = int(serie["year"])
        # Season/Episode falls schon bekannt (z.B. aus EPG-Subtitle geparst)
        if rec.get("season") is not None:
            api_body["season"] = int(rec["season"])
        if rec.get("episode") is not None:
            api_body["episode"] = int(rec["episode"])

        body = json.dumps(api_body).encode()
        resp, err = _api_call(f"{proxy_url}/api/record/start",
                              method="POST", body=body, timeout=10)
        if err or not resp:
            raise RuntimeError(f"record/start fehlgeschlagen: {err}")
        if not resp.get("ok"):
            raise RuntimeError(resp.get("message", "Unbekannter Fehler vom e2proxy"))

        proxy_rec_id        = resp["recording_id"]
        rec["proxy_rec_id"] = proxy_rec_id
        rec["filepath"]     = resp.get("file", rec["filepath"])
        rec["receiver"]     = resp.get("receiver", "")      # neu: welcher Tuner
        rec["shared_tuner"] = resp.get("shared_tuner", False)  # neu: shared tuner
        upsert_rec(rec)
        log.info(f"Proxy-RecID: {proxy_rec_id} Receiver: {rec['receiver']} — '{rec['title']}'")

        # ── Watchdog-Loop ──────────────────────────────────────────
        # Alle 30s: /api/record/status prüfen
        # a) Proxy meldet nicht mehr running → fertig, filepath aus Status holen
        # b) Deadline überschritten → aktiv stoppen, filepath aus Stop-Response
        # c) Zu viele Fehler → als done annehmen
        deadline     = rec["stop_ts"] + cfg("post_buffer_sec") + 120
        check_errors = 0
        max_errors   = 5
        final_filepath = rec.get("filepath", "")

        while True:
            time.sleep(30)
            now = time.time()

            running, remaining, status_data = _proxy_recording_running(proxy_url, proxy_rec_id)

            if running is None:
                check_errors += 1
                log.warning(f"Watchdog: Status-Check Fehler ({check_errors}/{max_errors}) '{rec['title']}'")
                if check_errors >= max_errors:
                    log.error(f"Watchdog: Aufgabe als fertig angenommen '{rec['title']}'")
                    break
                continue

            check_errors = 0

            if not running:
                # Filepath aus Start-Response verwenden
                if status_data and status_data.get("filename"):
                    fn = status_data.get("filename", "")
                    fp_from_start = rec.get("filepath", "")
                    if fn and fp_from_start and os.path.basename(fp_from_start) == fn:
                        final_filepath = fp_from_start
                    elif fn and fp_from_start:
                        dirpath = os.path.dirname(fp_from_start)
                        final_filepath = os.path.join(dirpath, fn)
                # Explizit Stop senden — stellt sicher dass Tuner freigegeben wird
                # (e2proxy-Watchdog hat evtl. schon gestoppt, aber doppelter Stop ist harmlos)
                log.info(f"Watchdog: Sende Stop zur Tuner-Freigabe '{rec['title']}'")
                _stop_proxy_recording(proxy_url, proxy_rec_id)
                time.sleep(3)  # kurz warten damit e2proxy Tuner freigibt
                log.info(f"Watchdog: Fertig — '{rec['title']}' → {final_filepath}")
                break

            if now > deadline:
                log.warning(f"Watchdog: Deadline, stoppe '{rec['title']}'")
                ok, fp = _stop_proxy_recording(proxy_url, proxy_rec_id)
                if fp:
                    final_filepath = fp
                time.sleep(5)
                break

            log.debug(f"Watchdog: '{rec['title']}' läuft ({remaining:.0f}s verbl.)")

        rec["status"]      = "done"
        rec["finished_at"] = datetime.now().isoformat()
        rec["filepath"]    = final_filepath
        # Dateigröße ermitteln (für Anzeige)
        try:
            if final_filepath and os.path.exists(final_filepath):
                rec["filesize"] = os.path.getsize(final_filepath)
        except Exception:
            pass
        log.info(f"Aufnahme fertig: '{rec['title']}' ({rec.get('filesize',0)//1024//1024} MB)")

    except Exception as e:
        rec["status"] = "failed"
        rec["error"]  = str(e)
        log.error(f"Aufnahme fehlgeschlagen: '{rec['title']}': {e}")

    upsert_rec(rec)
    if rec["status"] == "done" and cfg("cleanup_trigger") == "on_new":
        cleanup_old_recordings(rec["serie_id"])

    # Einmalige Aufnahme: Serie nach Aufnahme deaktivieren
    if rec["status"] in ("done", "failed"):
        serie = get_serie_by_id(rec.get("serie_id", ""))
        if serie and serie.get("once"):
            update_serie(serie["id"], {"enabled": False})
            log.info(f"Einmalige Aufnahme fertig — Serie '{serie['name']}' deaktiviert")

def recording_dispatcher():
    """Startet Aufnahmen zum richtigen Zeitpunkt."""
    log.info("Dispatcher gestartet")
    while True:
        try:
            now_ts = time.time()
            pre    = cfg("pre_buffer_sec")
            back2back_pre = 120  # 2 Min Vorlauf bei Back-to-Back auf gleichem Sender

            with _scheduled_lock:
                pending = [(k, v) for k, v in _scheduled.items()
                           if v.get("status") == "scheduled"]

            # Aktive Aufnahmen für Back-to-Back Erkennung
            active_recs = [r for r in get_recordings()
                           if r.get("status") == "recording"]

            for key, rec in pending:
                # Back-to-Back Bonus: läuft bereits Aufnahme auf gleichem Sender
                # die innerhalb von 60s endet wenn unsere starten würde?
                effective_pre = pre
                same_channel_active = [
                    r for r in active_recs
                    if r.get("channel_ref") == rec.get("channel_ref")
                ]
                for active in same_channel_active:
                    gap = rec["start_ts"] - active.get("stop_ts", 0)
                    if 0 <= gap <= 60:  # max 60s Lücke
                        effective_pre = max(pre, back2back_pre)
                        log.debug(
                            f"Back-to-Back: '{rec['title']}' folgt auf "
                            f"'{active['title']}' (gleicher Sender, {gap:.0f}s Lücke) — "
                            f"starte {effective_pre}s früher"
                        )
                        break

                if now_ts >= rec["start_ts"] - effective_pre:
                    with _scheduled_lock:
                        cur = _scheduled.get(key, {}).get("status")
                        if cur != "scheduled":
                            continue
                        _scheduled[key]["status"] = "recording"

                    t = threading.Thread(target=do_record, args=(rec,), daemon=True)
                    t.start()
        except Exception as e:
            log.error(f"Dispatcher: {e}")
        time.sleep(10)

# ── Cleanup ────────────────────────────────────────────────────────────────

def cleanup_old_recordings(serie_id):
    """
    Löscht alte Aufnahmen wenn keep_last überschritten.
    Löscht auch .nfo-Dateien (vom e2proxy geschrieben).
    """
    serie = get_serie_by_id(serie_id)
    if not serie:
        return
    keep_last = serie.get("keep_last", 0)
    if keep_last <= 0:
        return
    with _rec_lock:
        data = _load_recs()
        mine = [r for r in data
                if r.get("serie_id") == serie_id
                and r.get("status") == "done"
                and not r.get("protected", False)]
        mine.sort(key=lambda r: r.get("start_ts", 0))
        to_delete = mine[:-keep_last] if len(mine) > keep_last else []
        deleted = 0
        for rec in to_delete:
            fp = rec.get("filepath", "")
            if fp:
                # .ts Datei löschen
                try:
                    if os.path.exists(fp):
                        os.remove(fp)
                        log.info(f"Gelöscht: {fp}")
                        deleted += 1
                except Exception as e:
                    log.error(f"Löschen {fp}: {e}")
                # .nfo Datei löschen (gleicher Name, andere Endung)
                nfo = os.path.splitext(fp)[0] + ".nfo"
                try:
                    if os.path.exists(nfo):
                        os.remove(nfo)
                        log.info(f"Gelöscht: {nfo}")
                except Exception:
                    pass
            data = [r for r in data if r["id"] != rec["id"]]
        if deleted:
            log.info(f"Keep-Last Cleanup '{serie['name']}': {deleted} Aufnahme(n) gelöscht")
        _save_recs(data)

def _rescan_recordings():
    """
    Prüft alle 'recording' und verpassten 'scheduled' Einträge
    gegen den aktuellen Proxy-Status und aktualisiert die DB.
    Auch: bereinigt Einträge deren Datei nicht mehr existiert.
    """
    now_ts = time.time()
    updated = 0
    recs = get_recordings()

    for rec in recs:
        status = rec.get("status")
        changed = False

        # Laufende Aufnahmen: Proxy fragen
        if status == "recording":
            proxy_url    = rec.get("proxy_url")
            proxy_rec_id = rec.get("proxy_rec_id")
            if proxy_url and proxy_rec_id:
                running, remaining, status_data = _proxy_recording_running(proxy_url, proxy_rec_id)
                if running is False:
                    rec["status"]      = "done"
                    rec["finished_at"] = datetime.now().isoformat()
                    if status_data and status_data.get("filename"):
                        fp_known = rec.get("filepath","")
                        fn = status_data["filename"]
                        if fp_known and os.path.basename(fp_known) == fn:
                            pass  # Pfad ist korrekt
                        elif fp_known:
                            rec["filepath"] = os.path.join(os.path.dirname(fp_known), fn)
                    changed = True
                    log.info(f"Rescan: '{rec['title']}' jetzt als done markiert")

        # Verpasste scheduled
        elif status == "scheduled" and rec.get("stop_ts", 0) < now_ts - 300:
            rec["status"] = "missed"
            rec["error"]  = "Sendung verpasst"
            changed = True

        # Done: Datei prüfen ob noch vorhanden
        elif status == "done":
            fp = rec.get("filepath","")
            if not fp:
                # Kein Pfad bekannt → als file_missing markieren
                if not rec.get("file_missing"):
                    rec["file_missing"] = True
                    rec["error"] = "Kein Dateipfad bekannt (Aufnahme vor API-Umstellung)"
                    changed = True
                    log.warning(f"Rescan: Kein Pfad fuer '{rec.get('title','?')}'")
            else:
                exists = os.path.exists(fp)
                old_missing = rec.get("file_missing", False)
                rec["file_missing"] = not exists
                if exists:
                    try:
                        new_size = os.path.getsize(fp)
                        if new_size != rec.get("filesize", 0):
                            rec["filesize"] = new_size
                            changed = True
                    except Exception:
                        pass
                if old_missing != rec["file_missing"]:
                    changed = True
                    if not exists:
                        log.warning(f"Rescan: Datei fehlt: {fp}")
                    else:
                        log.info(f"Rescan: OK {fp} ({rec.get('filesize',0)//1024//1024} MB)")

        if changed:
            upsert_rec(rec)
            updated += 1

    log.info(f"Rescan abgeschlossen: {updated} Einträge aktualisiert")
    return updated




def _deduplicate_series_db():
    """Entfernt doppelte Serien (gleicher Name + gleicher channel_ref)."""
    with _series_lock:
        data = _load_series()
        before = len(data)
        seen = {}
        unique = []
        for s in data:
            key = f"{s.get('channel_ref','')}:{s.get('name','').lower()}"
            if key not in seen:
                seen[key] = True
                unique.append(s)
        if len(unique) < before:
            _save_series(unique)
            log.info(f"Serien dedupliziert: {before - len(unique)} Duplikate entfernt")
        return before - len(unique)


def _deduplicate_recordings_db():
    """Entfernt Duplikate aus der DB — behält pro event_key den besten Eintrag."""
    PRIO = {"done": 5, "recording": 4, "failed": 3, "skipped": 2, "scheduled": 1}
    with _rec_lock:
        data = _load_recs()
        before = len(data)
        seen = {}
        for rec in data:
            key = _event_key(rec.get("channel_ref",""), rec.get("start_ts", 0))
            existing = seen.get(key)
            if not existing or PRIO.get(rec.get("status",""), 0) > PRIO.get(existing.get("status",""), 0):
                seen[key] = rec
        deduped = list(seen.values())
        removed = before - len(deduped)
        if removed > 0:
            _save_recs(deduped)
            log.info(f"Dedupliziert: {removed} Duplikate entfernt ({before} → {len(deduped)})")
        return removed


def cleanup_all_series():
    log.info("Cleanup läuft...")
    for s in get_series():
        cleanup_old_recordings(s["id"])

def daily_cleanup_scheduler():
    while True:
        time.sleep(60)
        if cfg("cleanup_trigger") != "daily":
            continue
        now = datetime.now()
        if now.hour == int(cfg("cleanup_hour")) and now.minute == 0:
            cleanup_all_series()


# ── Web UI CSS ─────────────────────────────────────────────────────────────

_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;700&display=swap');
:root{
  --bg:#0a0a0f;--surface:#12121a;--surface2:#1a1a26;
  --border:#2a2a3d;--accent:#6366f1;--accent2:#818cf8;
  --text:#e2e2f0;--muted:#6b6b8a;
  --green:#22c55e;--red:#ef4444;--yellow:#f59e0b;--blue:#818cf8;
}
[data-theme="light"]{
  --bg:#f4f5f7;--surface:#ffffff;--surface2:#f0f1f5;
  --border:#d1d5db;--accent:#4f46e5;--accent2:#4f46e5;
  --text:#1a1a2e;--muted:#6b7280;
  --green:#16a34a;--red:#dc2626;--yellow:#d97706;--blue:#4f46e5;
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Inter',system-ui,sans-serif;font-weight:300;background:var(--bg);color:var(--text);min-height:100vh}
header{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;gap:12px;position:sticky;top:0;z-index:100}
.logo{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:16px;color:var(--accent2)}
.logo span{color:var(--muted);font-weight:400}
header .sub{color:var(--muted);font-size:.78rem;font-family:'JetBrains Mono',monospace}
nav{display:flex;gap:0;margin-left:auto;align-items:center}
nav button{background:none;border:none;color:var(--muted);padding:7px 14px;cursor:pointer;font-size:.82rem;font-family:'JetBrains Mono',monospace;border-bottom:2px solid transparent;transition:all .15s}
nav button:hover,nav button.active{color:var(--text);border-bottom-color:var(--accent2)}
.container{padding:20px 24px;max-width:1400px;margin:0 auto}
.tab{display:none}.tab.active{display:block}
#tab-help .help-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
#tab-help .help-grid-full{grid-column:1/-1}
#tab-help p{font-size:.82rem;line-height:1.7;color:var(--muted)}
#tab-help p strong{color:var(--text)}
#tab-help code{font-family:'JetBrains Mono',monospace;font-size:.78rem;color:var(--accent2);background:rgba(99,102,241,.08);padding:1px 5px;border-radius:3px}
#tab-help .version-badge{display:inline-block;font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700;color:var(--accent2);background:rgba(99,102,241,.12);border:1px solid rgba(99,102,241,.3);padding:3px 10px;border-radius:4px;margin-bottom:16px}
#tab-help .cl-entry{padding:10px 0;border-bottom:1px solid var(--border)}
#tab-help .cl-entry:last-child{border-bottom:none}
#tab-help .cl-current{border-left:3px solid var(--accent);padding-left:14px}
#tab-help .cl-old{border-left:3px solid var(--border);padding-left:14px}
@media(max-width:700px){#tab-help .help-grid{grid-template-columns:1fr}}
.card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:18px 20px;margin-bottom:14px}
.card-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}
.card-title{font-size:.9rem;font-weight:600;font-family:'JetBrains Mono',monospace;color:var(--accent2)}
table{width:100%;border-collapse:collapse}
th{text-align:left;padding:8px 10px;color:var(--muted);font-size:.72rem;font-family:'JetBrains Mono',monospace;text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid var(--border)}
td{padding:9px 10px;border-bottom:1px solid var(--border);font-size:.85rem;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:var(--surface2)}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:.72rem;font-weight:600;font-family:'JetBrains Mono',monospace}
.badge-scheduled{background:rgba(129,140,248,.15);color:var(--blue)}
.badge-recording{background:rgba(99,102,241,.25);color:var(--accent2)}
.badge-done{background:rgba(34,197,94,.12);color:var(--green)}
.badge-failed{background:rgba(239,68,68,.12);color:var(--red)}
.badge-skipped{background:rgba(107,107,138,.1);color:var(--muted)}
.badge-missed{background:rgba(245,158,11,.12);color:var(--yellow)}
.badge-unknown{background:rgba(107,107,138,.1);color:var(--muted)}
.btn{display:inline-flex;align-items:center;gap:5px;padding:6px 12px;border-radius:6px;border:1px solid var(--border);background:var(--surface2);color:var(--text);cursor:pointer;font-size:.78rem;font-family:'JetBrains Mono',monospace;transition:all .15s;white-space:nowrap}
.btn:hover{border-color:var(--accent);color:var(--accent2)}
.btn-primary{background:var(--accent);color:#fff;border-color:var(--accent)}
.btn-primary:hover{background:var(--accent2);border-color:var(--accent2);color:#fff}
.btn-danger{border-color:var(--red);color:var(--red)}
.btn-danger:hover{background:var(--red);color:#fff}
.btn-sm{padding:3px 8px;font-size:.72rem}
label{display:block;color:var(--muted);font-size:.72rem;font-family:'JetBrains Mono',monospace;margin-bottom:3px;letter-spacing:.03em}
input,select,textarea{width:100%;background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:7px 10px;font-size:.85rem;font-family:inherit;outline:none;transition:border-color .15s}
input:focus,select:focus{border-color:var(--accent)}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px}
.form-row.one{grid-template-columns:1fr}
.form-row.three{grid-template-columns:1fr 1fr 1fr}
.stat-row{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:18px}
.stat-box{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px 18px;min-width:110px;text-align:center}
.stat-box .num{font-size:1.7rem;font-weight:700;color:var(--accent2);font-family:'JetBrains Mono',monospace}
.stat-box .lbl{font-size:.72rem;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:2px}
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:100;align-items:center;justify-content:center}
.modal-overlay.open{display:flex}
.modal{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:22px;width:min(680px,95vw);max-height:90vh;overflow-y:auto}
.modal h2{margin-bottom:18px;color:var(--accent2);font-family:'JetBrains Mono',monospace;font-size:.95rem}
.actions{display:flex;gap:8px;justify-content:flex-end;margin-top:14px}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block}
.dot-green{background:var(--green)}.dot-red{background:var(--red)}.dot-yellow{background:var(--yellow)}.dot-blue{background:var(--blue)}
/* Plan */
.plan-day-tabs{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:12px}
.plan-day-tab{background:none;border:1px solid var(--border);color:var(--muted);padding:4px 11px;border-radius:5px;cursor:pointer;font-size:.75rem;font-family:'JetBrains Mono',monospace;transition:all .15s}
.plan-day-tab.active{background:var(--surface2);border-color:var(--accent);color:var(--text)}
.plan-outer{border:1px solid var(--border);border-radius:6px;overflow:hidden}
.plan-scroll-wrap{overflow-x:auto;overflow-y:visible;position:relative}
.plan-time-header{position:sticky;top:0;z-index:3;background:var(--surface);border-bottom:1px solid var(--border);height:24px}
.plan-grid{display:table;border-collapse:collapse}
.plan-ch-row{display:table-row}
.plan-ch-label{display:table-cell;width:140px;min-width:140px;position:sticky;left:0;z-index:1;background:var(--surface);border-right:1px solid var(--border);border-bottom:1px solid var(--border);padding:5px 8px;vertical-align:middle}
.plan-ch-label-inner{display:flex;align-items:center;gap:6px}
.plan-ch-logo{width:28px;height:20px;object-fit:contain;flex-shrink:0;filter:brightness(.85)}
.plan-ch-name{font-size:.7rem;color:var(--muted);font-family:'JetBrains Mono',monospace;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:90px}
.plan-ch-events{display:table-cell;vertical-align:middle;border-bottom:1px solid var(--border);padding:3px 0 3px 4px;white-space:nowrap}
.epg-slot{display:inline-flex;align-items:center;border-radius:4px;padding:3px 6px;font-size:.7rem;font-family:'JetBrains Mono',monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;vertical-align:middle;border:1px solid transparent}
.epg-slot.normal{background:rgba(107,107,138,.08);color:var(--muted);border-color:rgba(107,107,138,.15)}
.epg-slot.match{background:rgba(99,102,241,.18);color:var(--accent2);border-color:rgba(99,102,241,.4);font-weight:600}
.epg-slot.live{background:rgba(34,197,94,.12);color:var(--green);border-color:rgba(34,197,94,.35);font-weight:600}
.epg-slot.clickable{cursor:pointer}.epg-slot.clickable:hover{filter:brightness(1.3)}
.plan-legend{display:flex;gap:14px;margin-top:8px;font-size:.72rem;font-family:'JetBrains Mono',monospace;color:var(--muted)}
.plan-legend span{display:flex;align-items:center;gap:4px}
.legend-box{width:11px;height:11px;border-radius:2px}
.epg-tooltip{position:fixed;z-index:1000;background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:10px 14px;max-width:300px;font-size:.8rem;pointer-events:auto;display:none;box-shadow:0 4px 20px rgba(0,0,0,.4)}
.epg-tooltip .tt-title{font-weight:600;font-family:'JetBrains Mono',monospace;margin-bottom:3px;color:var(--text)}
.epg-tooltip .tt-time{color:var(--muted);font-size:.72rem;font-family:'JetBrains Mono',monospace;margin-bottom:4px}
.epg-tooltip .tt-sub{color:var(--blue);font-size:.75rem;margin-bottom:3px}
.epg-tooltip .tt-desc{color:var(--muted);font-size:.75rem;line-height:1.4;max-height:60px;overflow:hidden}
.epg-tooltip .tt-rec{color:var(--accent2);font-size:.72rem;font-family:'JetBrains Mono',monospace;margin-top:4px;font-weight:600}
/* Proxy */
.proxy-card{background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:12px 16px;margin-bottom:8px;display:flex;align-items:center;justify-content:space-between}
.proxy-card.online{border-color:rgba(34,197,94,.3)}
.proxy-card.offline{border-color:rgba(239,68,68,.2)}
.proxy-name{font-weight:600;font-size:.85rem;font-family:'JetBrains Mono',monospace;color:var(--text)}
.proxy-url{font-size:.72rem;color:var(--muted);font-family:'JetBrains Mono',monospace}
.tuner-bar{display:flex;gap:4px;margin-top:5px;align-items:center}
.tuner-dot{width:11px;height:11px;border-radius:50%}
.tuner-dot.free{background:var(--green)}.tuner-dot.busy{background:var(--yellow)}
/* Progress */
.rec-progress{height:2px;background:var(--border);border-radius:1px;margin-top:3px;overflow:hidden}
.rec-progress-bar{height:100%;background:var(--accent);border-radius:1px;transition:width .5s}
/* TMDB */
.tmdb-results{display:flex;flex-direction:column;gap:6px;max-height:250px;overflow-y:auto;margin-top:6px}
.tmdb-item{display:flex;align-items:center;gap:10px;padding:8px 10px;border:1px solid var(--border);border-radius:6px;cursor:pointer;transition:border-color .15s}
.tmdb-item:hover{border-color:var(--accent)}.tmdb-item.selected{border-color:var(--accent);background:rgba(99,102,241,.08)}
.tmdb-poster{width:32px;height:48px;border-radius:3px;object-fit:cover;background:var(--border);flex-shrink:0}
.tmdb-info{flex:1;min-width:0}
.tmdb-info .tname{font-size:.85rem;font-weight:600}
.tmdb-info .tmeta{font-size:.72rem;color:var(--muted);font-family:'JetBrains Mono',monospace}
.tmdb-info .tregex{font-size:.7rem;color:var(--blue);margin-top:2px;font-family:'JetBrains Mono',monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ch-picker-list{max-height:150px;overflow-y:auto;border:1px solid var(--border);border-radius:5px;background:var(--bg);margin-top:5px}
.ch-picker-item{padding:6px 10px;cursor:pointer;font-size:.82rem;border-bottom:1px solid var(--border)}
.ch-picker-item:hover{background:var(--surface2);color:var(--accent2)}
.ch-picker-item:last-child{border-bottom:none}
.selected-ch{display:flex;align-items:center;justify-content:space-between;padding:7px 10px;background:rgba(99,102,241,.08);border:1px solid var(--accent);border-radius:5px;font-size:.85rem;margin-top:5px}
.help-text{font-size:.72rem;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:3px}
.regex-preview{font-family:'JetBrains Mono',monospace;font-size:.8rem;color:var(--accent2);padding:6px 10px;background:rgba(99,102,241,.05);border:1px solid rgba(99,102,241,.2);border-radius:5px;margin-top:4px;word-break:break-all}
.log-entry{font-family:'JetBrains Mono',monospace;font-size:.75rem;padding:2px 0;border-bottom:1px solid var(--border)}
.log-entry .ts{color:var(--muted);margin-right:6px}
.level-INFO{color:var(--blue)}.level-WARNING{color:var(--yellow)}.level-ERROR{color:var(--red)}.level-DEBUG{color:var(--muted)}
.empty{text-align:center;padding:36px;color:var(--muted);font-family:'JetBrains Mono',monospace;font-size:.8rem}
hr{border:none;border-top:1px solid var(--border)}
.toast{position:fixed;bottom:20px;right:20px;background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:9px 14px;border-radius:7px;font-size:.78rem;font-family:'JetBrains Mono',monospace;opacity:0;transform:translateY(8px);transition:all .25s;z-index:9999;pointer-events:none}
.toast.show{opacity:1;transform:translateY(0)}
.toast.success{border-color:var(--green);color:var(--green)}
.toast.error{border-color:var(--red);color:var(--red)}
"""


# ── Web UI JS ──────────────────────────────────────────────────────────────

_JS = """
function setTheme(theme) {
  if (theme === 'light') {
    document.documentElement.setAttribute('data-theme', 'light');
    localStorage.setItem('e2recorder-theme', 'light');
  } else {
    document.documentElement.removeAttribute('data-theme');
    localStorage.setItem('e2recorder-theme', 'dark');
  }
  updateThemeButtons();
}

function toggleTheme() {
  const isLight = document.documentElement.getAttribute('data-theme') === 'light';
  setTheme(isLight ? 'dark' : 'light');
}

function updateThemeButtons() {
  const isLight = document.documentElement.getAttribute('data-theme') === 'light';
  const hdr = document.getElementById('theme-toggle-btn');
  if (hdr) hdr.innerHTML = isLight ? '&#9728;' : '&#127769;';  // ☀ / 🌙
}



function showToast(msg, type) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast show' + (type ? ' ' + type : '');
  setTimeout(() => t.className = 'toast', 3000);
}

let _channels = [];
let _selRef = '', _selName = '';
let _scheduleData = [];
let _activePlanDay = 0;
let _selectedTmdbId = null;
let _selectedTmdbPoster = '';
const PX_PER_MIN = 3;

function switchTab(name, btn) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  btn.classList.add('active');
  if (name === 'plan' || name === 'overview') loadPlan();
  if (name === 'series')     loadSeries();
  if (name === 'movies')     loadSeries();
  if (name === 'recordings') loadRecordings();
  // proxies now in settings
  if (name === 'settings')   { loadConfig(); setTimeout(loadLogs, 300); setTimeout(loadTunerHistory, 400); }
}

// ── Status ────────────────────────────────────────────────
async function loadStatus() {
  try {
    const resp = await fetch('/api/status');
    if (!resp.ok) return;
    const d = await resp.json();
    const rc = d.recordings || {};
    const hdr = document.getElementById('proxy-status-header');
    const proxyCount = (d.proxies_online||0);
    if (hdr) hdr.textContent = proxyCount > 0
      ? t('hdr.proxy_connected', {n: proxyCount})
      : t('hdr.no_proxy');
  } catch(e) {}
}

// ── Aufnahmeplan ──────────────────────────────────────────
async function loadPlan() {
  const lookahead = +document.getElementById('plan-hours').value;
  try {
    const ctrl = new AbortController();
    const tid = setTimeout(() => ctrl.abort(), 8000);
    const r = await fetch('/api/schedule', {signal: ctrl.signal});
    clearTimeout(tid);
    if (r.ok) _scheduleData = await r.json();
  } catch(e) {
    console.warn('loadPlan failed:', e);
    // Don't clear existing data on error
  }
  buildDayTabs(lookahead);
  renderPlan();
}

function midnight(dayOffset) {
  const d = new Date(); d.setHours(0,0,0,0); d.setDate(d.getDate() + dayOffset);
  return d.getTime() / 1000;
}

function buildDayTabs(lookahead) {
  const tabs = document.getElementById('plan-day-tabs');
  const now = new Date();
  const days = [];
  for (let d = 0; d < Math.ceil(lookahead / 24); d++) {
    const dt = new Date(now); dt.setDate(dt.getDate() + d);
    days.push(dt.toLocaleDateString(LC(), {weekday:'short', day:'2-digit', month:'2-digit'}));
  }
  if (_activePlanDay >= days.length) _activePlanDay = 0;
  tabs.innerHTML = days.map((d,i) =>
    `<button class="plan-day-tab${i===_activePlanDay?' active':''}" onclick="setDay(${i})">${d}</button>`
  ).join('');
}

function setDay(i) {
  _activePlanDay = i;
  document.querySelectorAll('.plan-day-tab').forEach((b,j) => b.classList.toggle('active', i===j));
  renderPlan();
}

function getWinStart() {
  const now = Date.now() / 1000;
  if (_activePlanDay === 0) return Math.floor(now / 3600) * 3600;
  return midnight(_activePlanDay) + 6 * 3600;
}

function renderTimeHeader(winStart, winEnd) {
  const hdr = document.getElementById('plan-time-header');
  const totalPx = ((winEnd - winStart) / 60) * PX_PER_MIN;
  let inner = `<div style="position:relative;height:24px;min-width:${totalPx + 140}px">`;
  inner += `<div style="position:absolute;left:0;width:140px;height:100%;background:var(--surface);border-right:1px solid var(--border);z-index:1"></div>`;
  let t = Math.ceil(winStart / 3600) * 3600;
  while (t <= winEnd) {
    const px = ((t - winStart) / 60) * PX_PER_MIN + 140;
    const label = new Date(t * 1000).toLocaleTimeString(LC(), {hour:'2-digit', minute:'2-digit'});
    inner += `<div style="position:absolute;left:${px}px;transform:translateX(-50%);font-size:10px;color:var(--muted);white-space:nowrap;line-height:24px">${label}</div>`;
    inner += `<div style="position:absolute;left:${px}px;top:0;bottom:0;width:1px;background:var(--border);opacity:.4"></div>`;
    t += 3600;
  }
  inner += '</div>';
  hdr.innerHTML = inner;
}

function renderPlan() {
  const grid = document.getElementById('plan-grid');
  if (!_scheduleData.length) {
    grid.innerHTML = '<div class="empty">' + t('plan.empty_noepg') + '</div>';
    return;
  }
  const lookahead = +document.getElementById('plan-hours').value;
  const now = Date.now() / 1000;
  const winStart = getWinStart();
  const winEnd   = winStart + lookahead * 3600;
  const totalPx  = ((winEnd - winStart) / 60) * PX_PER_MIN;
  renderTimeHeader(winStart, winEnd);
  const nowPx = ((now - winStart) / 60) * PX_PER_MIN;
  const nowLine = now > winStart && now < winEnd
    ? `<div style="position:absolute;top:0;bottom:0;left:${nowPx}px;width:2px;background:var(--accent);z-index:2;pointer-events:none"></div>`
    : '';
  let rows = '';
  for (const ch of _scheduleData) {
    const evs = ch.events.filter(e => e.stop > winStart && e.start < winEnd);
    // Sender ohne EPG (z.B. neu hinzugefügt) trotzdem als leere Zeile zeigen
    if (!evs.length && !ch.no_epg) continue;
    const logo = ch.logo
      ? `<img class="plan-ch-logo" src="${escHtml(ch.logo)}" alt="" onerror="this.style.display='none'">`
      : '';
    // Lesbaren Namen anzeigen - channel_name bevorzugt, sonst aus Favoriten-Cache
    const displayName = ch.channel_name || _chNameById(ch.channel_id) || ch.channel_id;
    const label = `<div class="plan-ch-label"><div class="plan-ch-label-inner">${logo}<span class="plan-ch-name">${escHtml(displayName)}</span></div></div>`;
    let slots = `<div style="position:relative;height:40px;min-width:${totalPx}px">${nowLine}`;
    if (!evs.length) {
      slots += `<div style="position:absolute;left:8px;top:50%;transform:translateY(-50%);font-size:11px;color:var(--muted);font-style:italic">${t('plan.no_epg')}</div>`;
    }
    for (const ev of evs) {
      const s = Math.max(ev.start, winStart);
      const e = Math.min(ev.stop, winEnd);
      const leftPx  = ((s - winStart) / 60 * PX_PER_MIN).toFixed(1);
      const widthPx = Math.max(((e - s) / 60 * PX_PER_MIN) - 2, 4).toFixed(1);
      const isLive  = ev.start <= now && ev.stop > now;
      const cls = (isLive && ev.matched) ? 'match live' : (ev.matched ? 'match' : 'normal');
      const prefix = ev.matched ? '&#11044; ' : '';
      const clickable = ev.stop > now && !ev.matched;
      // Base64-encode JSON um Sonderzeichen in HTML-Attributen zu vermeiden
      const evJson = btoa(encodeURIComponent(JSON.stringify({
        start: ev.start, stop: ev.stop, title: ev.title,
        subtitle: ev.subtitle||'', desc: ev.desc||'',
        matched: ev.matched, serie_name: ev.serie_name||'', rec_id: ev.rec_id||null
      })));
      slots += `<div class="epg-slot ${cls}${clickable?' clickable':''}"
        style="position:absolute;top:4px;bottom:4px;left:${leftPx}px;width:${widthPx}px"
        data-ev="${evJson}" data-chname="${encodeURIComponent(ch.channel_name)}" data-chid="${encodeURIComponent(ch.channel_id||ch.channel_name)}"
        onmouseenter="showTooltip(event,this)" onmouseleave="hideTooltip()"
        ${clickable?'onclick="quickRecordFromEl(this)"':''}>${prefix}${escHtml(ev.title)}</div>`;
    }
    slots += '</div>';
    rows += `<div class="plan-ch-row">${label}<div class="plan-ch-events">${slots}</div></div>`;
  }
  grid.innerHTML = rows || '<div class="empty">' + t('plan.empty_window') + '</div>';
  if (_activePlanDay === 0 && nowPx > 200) {
    const wrap = document.getElementById('plan-scroll-wrap');
    if (wrap) setTimeout(() => wrap.scrollLeft = nowPx - 200, 50);
  }
}

function showTooltip(evt, el) {
  const ev = JSON.parse(decodeURIComponent(atob(el.dataset.ev)));
  const tt = document.getElementById('epg-tooltip');
  const s = new Date(ev.start*1000).toLocaleTimeString(LC(),{hour:'2-digit',minute:'2-digit'});
  const e = new Date(ev.stop*1000).toLocaleTimeString(LC(),{hour:'2-digit',minute:'2-digit'});
  const dur = Math.round((ev.stop-ev.start)/60);
  const now = Date.now()/1000;
  const future = ev.stop > now;
  tt.innerHTML = `
    <div class="tt-title">${escHtml(ev.title)||t('tt.no_title')}</div>
    <div class="tt-time">${s} – ${e} (${dur} min)</div>
    ${ev.subtitle?`<div class="tt-sub">${escHtml(ev.subtitle)}</div>`:''}
    ${ev.desc?`<div class="tt-desc">${escHtml(ev.desc)}</div>`:`<div class="tt-desc" style="font-style:italic">${t('tt.no_desc')}</div>`}
    ${ev.matched ? `<div class="tt-rec">&#11044; ${t('tt.recording',{n:escHtml(ev.serie_name)})}</div>` : ''}
    ${!ev.matched && ev.stop > Date.now()/1000 ? `<div style="color:var(--muted);font-size:.72rem;margin-top:4px">${t('tt.click_record')}</div>` : ''}
  `;
  // Auslassen-Button separat unter dem Inhalt — immer sichtbar
  if (ev.matched && ev.rec_id) {
    tt.innerHTML += `<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border)">
      <button onclick="skipRecording('${ev.rec_id}',event)"
        style="width:100%;background:rgba(218,54,51,.15);color:var(--red);border:1px solid var(--red);
               border-radius:4px;padding:4px 8px;font-size:.75rem;cursor:pointer">
        &#10005; ${t('tt.skip')}
      </button>
    </div>`;
  }
  // Tooltip am EPG-Slot positionieren (damit Maus rüberfahren kann)
  const slotRect = el.getBoundingClientRect();
  const ttWidth = 290;
  const left = Math.min(evt.clientX, window.innerWidth - ttWidth - 10);
  // Erst sichtbar (unsichtbar) rendern, um die echte Höhe zu messen
  tt.style.left = left + 'px';
  tt.style.visibility = 'hidden';
  tt.style.display = 'block';
  const ttHeight = tt.offsetHeight;
  const spaceAbove = slotRect.top;
  let top = spaceAbove > ttHeight + 10
    ? slotRect.top - ttHeight - 8   // über dem Slot
    : slotRect.bottom + 8;           // unter dem Slot
  // In den Viewport zwingen — Button bleibt immer sichtbar
  if (top + ttHeight > window.innerHeight - 10) top = window.innerHeight - ttHeight - 10;
  if (top < 10) top = 10;
  tt.style.top = top + 'px';
  tt.style.visibility = 'visible';
  // Tooltip offen halten wenn Maus drüber fährt
  tt.onmouseleave = () => tt.style.display = 'none';
}
function hideTooltip() {
  // Kurze Verzögerung — gibt Zeit zum Rüberfahren
  setTimeout(() => {
    const tt = document.getElementById('epg-tooltip');
    // Nicht verstecken wenn Maus auf dem Tooltip ist
    if (!tt.matches(':hover')) tt.style.display = 'none';
  }, 120);
}

async function skipRecording(recId, evt) {
  evt.stopPropagation(); hideTooltip();
  await fetch('/api/schedule/'+recId, {method:'DELETE'});
  await loadPlan(); loadStatus();
}
function quickRecordFromEl(el) {
  const ev     = JSON.parse(decodeURIComponent(atob(el.dataset.ev)));
  const chName = decodeURIComponent(el.dataset.chname || '');
  const chId   = decodeURIComponent(el.dataset.chid   || '');
  quickRecord(ev, chName, chId);
}

async function triggerScan(btn) {
  if (btn) { btn.innerHTML = '&#9203; ' + t('plan.loading'); btn.disabled = true; }
  await fetch('/api/scan', {method:'POST'});
  for (let i = 0; i < 15; i++) {
    await new Promise(r => setTimeout(r, 2000));
    const s = await fetch('/api/status').then(r => r.json());
    if (!s.scan_status?.running) break;
  }
  await refreshChannels();
  await loadPlan();
  if (btn) { btn.innerHTML = '&#8635; ' + t('plan.scan'); btn.disabled = false; }
}

async function refreshChannels() {
  // Senderliste (Favoriten) neu vom Proxy holen, damit neue/geänderte Sender
  // sofort in Plan-Anzeige und Auswahl-Dropdown erscheinen.
  try { _channels = await fetch('/api/channels').then(r => r.json()); } catch(e) {}
}

async function quickRecord(ev, chName, chRef) {
  const startStr = new Date(ev.start*1000).toLocaleString(LC(),{weekday:'short',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});
  const stopStr  = new Date(ev.stop*1000).toLocaleTimeString(LC(),{hour:'2-digit',minute:'2-digit'});
  const dur      = Math.round((ev.stop - ev.start) / 60);
  const titleWords = ev.title.split(/\\s+/);
  const shortTitle = titleWords.slice(0, Math.min(3, titleWords.length)).join(' ');
  const overlay = document.createElement('div');
  overlay.id = 'qr-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.8);z-index:200;display:flex;align-items:center;justify-content:center';
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px;max-width:520px;width:92%">
      <div style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--muted);margin-bottom:8px">${escHtml(chName)} &middot; ${startStr} &ndash; ${stopStr} (${dur} min)</div>
      <div style="font-size:1rem;font-weight:600;margin-bottom:3px">${escHtml(ev.title)}</div>
      ${ev.subtitle ? `<div style="color:var(--blue);font-size:.8rem;margin-bottom:3px">${escHtml(ev.subtitle)}</div>` : ''}
      ${ev.desc ? `<div style="color:var(--muted);font-size:.76rem;line-height:1.4;margin-bottom:12px;max-height:56px;overflow:hidden">${escHtml(ev.desc)}</div>` : '<div style="margin-bottom:12px"></div>'}

      <!-- Typ-Auswahl -->
      <div style="display:flex;gap:8px;margin-bottom:14px">
        <button id="qr-kind-movie" onclick="qrSetKind('movie')"
          class="btn" style="flex:1;padding:10px">&#127909; ${t('qr.movie')}</button>
        <button id="qr-kind-series" onclick="qrSetKind('series')"
          class="btn" style="flex:1;padding:10px">&#128250; ${t('qr.series')}</button>
      </div>

      <!-- Film-Optionen (anfangs versteckt) -->
      <div id="qr-movie-box" style="display:none;border:1px solid var(--border);border-radius:6px;padding:14px;margin-bottom:10px;background:var(--surface2)">
        <button onclick="confirmQuickRecord('${escAttr(ev.title)}','${escAttr(chName)}','${escAttr(chRef)}',${ev.start},${ev.stop},'${escAttr(ev.subtitle||'')}','movie','once')"
          class="btn btn-primary" style="width:100%;font-size:.9rem;padding:10px">&#11044; ${t('qr.rec_movie')}</button>
        <div style="font-size:.7rem;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:6px;text-align:center">
          ${t('qr.movie_note')}
        </div>
      </div>

      <!-- Serien-Optionen (anfangs versteckt) -->
      <div id="qr-series-box" style="display:none">
        <div style="border:1px solid var(--border);border-radius:6px;padding:14px;margin-bottom:10px;background:var(--surface2)">
          <button onclick="confirmQuickRecord('${escAttr(ev.title)}','${escAttr(chName)}','${escAttr(chRef)}',${ev.start},${ev.stop},'${escAttr(ev.subtitle||'')}','series','once')"
            class="btn btn-primary" style="width:100%;font-size:.9rem;padding:10px">&#11044; ${t('qr.this_episode')}</button>
          <div style="font-size:.7rem;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:6px;text-align:center">
            ${t('qr.once_note')}
          </div>
        </div>
        <div style="border:1px solid var(--border);border-radius:6px;padding:12px;background:var(--surface2)">
          <div style="font-family:'JetBrains Mono',monospace;font-size:.7rem;color:var(--muted);margin-bottom:8px">&#8635; ${t('qr.all_episodes')}</div>
          <label style="font-size:.7rem;color:var(--muted);font-family:'JetBrains Mono',monospace;display:block;margin-bottom:3px">${t('qr.pattern')}</label>
          <input id="qr-regex" type="text" value="${escHtml(shortTitle)}"
            style="width:100%;background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:5px;padding:6px 8px;font-size:.8rem;font-family:'JetBrains Mono',monospace;outline:none;margin-bottom:6px">
          <div style="font-size:.7rem;color:var(--muted);margin-bottom:8px">
            ${t('qr.full_title')} <code style="color:var(--accent2)">${escHtml(ev.title)}</code><br>
            ${t('qr.shorten')} <code style="color:var(--accent2)">${escHtml(shortTitle)}</code>
          </div>
          <button onclick="confirmQuickRecord('${escAttr(ev.title)}','${escAttr(chName)}','${escAttr(chRef)}',${ev.start},${ev.stop},'${escAttr(ev.subtitle||'')}','series','recurring')"
            class="btn btn-primary" style="width:100%;background:var(--surface);border-color:var(--accent2);color:var(--accent2)">&#8635; ${t('qr.rec_all')}</button>
        </div>
      </div>

      <div style="display:flex;justify-content:flex-end;margin-top:10px">
        <button onclick="document.getElementById('qr-overlay')?.remove()" class="btn">${t('common.close')}</button>
      </div>
    </div>
  `;
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}


function qrSetKind(kind) {
  const m = document.getElementById('qr-kind-movie');
  const s = document.getElementById('qr-kind-series');
  const mb = document.getElementById('qr-movie-box');
  const sb = document.getElementById('qr-series-box');
  if (kind === 'movie') {
    m.style.background = 'var(--accent)';
    m.style.color = 'white';
    s.style.background = '';
    s.style.color = '';
    mb.style.display = 'block';
    sb.style.display = 'none';
  } else {
    s.style.background = 'var(--accent)';
    s.style.color = 'white';
    m.style.background = '';
    m.style.color = '';
    sb.style.display = 'block';
    mb.style.display = 'none';
  }
}

async function confirmQuickRecord(title, chName, chRef, startTs, stopTs, subtitle, kind, mode) {
  // kind: "movie" | "series"
  // mode: "once" = einmalige Aufnahme, "recurring" = Regex-Serie
  const isOnce = (mode === 'once');
  const regex = isOnce
    ? title
    : (document.getElementById('qr-regex')?.value || title);
  document.getElementById('qr-overlay')?.remove();
  const r = await fetch('/api/series/from-epg', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({
      title, channel_name:chName, channel_ref:chRef,
      start_ts:startTs, stop_ts:stopTs, subtitle,
      regex_pattern: regex,
      once: isOnce,
      kind: kind,
    })
  });
  const d = await r.json();
  if (d.ok) {
    const msg = d.duplicate
      ? t('toast.dup_planned')
      : (isOnce
          ? (kind === 'movie' ? t('toast.movie_planned') : t('toast.once_planned'))
          : t('toast.series_created'));
    showToast(msg, d.duplicate ? '' : 'success');
    await loadPlan(); loadStatus();
  } else {
    showToast(t('toast.error',{n:(d.error||'?')}), 'error');
  }
}

// ── Serien ────────────────────────────────────────────────
async function loadSeries() {
  const all = await fetch('/api/series').then(r => r.json());
  const movies = all.filter(s => s.kind === 'movie');
  const series = all.filter(s => (s.kind || 'series') === 'series');
  renderSeriesTable(series, 'series-tbody');
  renderSeriesTable(movies, 'movies-tbody');
}

function renderSeriesTable(series, tbodyId) {
  const tbody = document.getElementById(tbodyId);
  if (!tbody) return;
  if (!series.length) { tbody.innerHTML='<tr><td colspan="7" class="empty">' + t('table.empty') + '</td></tr>'; return; }
  tbody.innerHTML = series.map(s => {
    const isMovie = s.kind === 'movie';
    const kindBadge = isMovie
      ? `<span class="badge" style="background:rgba(245,158,11,.15);color:var(--yellow)">&#127909; ${t('badge.movie')}</span>`
      : `<span class="badge" style="background:rgba(99,102,241,.15);color:var(--accent2)">&#128250; ${t('badge.series')}</span>`;
    const offsetInfo = (s.pre_offset_sec || s.post_offset_sec)
      ? `<div style="font-size:.68rem;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:2px">${s.pre_offset_sec>0?'+':''}${s.pre_offset_sec||0}s / ${s.post_offset_sec>0?'+':''}${s.post_offset_sec||0}s</div>`
      : '';
    return `<tr>
    <td><strong>${escHtml(s.name)}</strong>${s.year?` <span style="color:var(--muted)">(${s.year})</span>`:''}${offsetInfo}</td>
    <td>${kindBadge}</td>
    <td>${escHtml(s.channel_name)}</td>
    <td style="font-family:monospace;font-size:.8rem;color:var(--blue)">${escHtml(s.regex_pattern||'')}</td>
    <td>${s.keep_last===0?`<span style="color:var(--muted)">${t('common.all')}</span>`:s.keep_last}</td>
    <td>${s.once
      ? (s.enabled
          ? `<span class="badge" style="background:rgba(99,102,241,.15);color:var(--accent2)">${t('badge.once')}</span>`
          : `<span class="badge" style="background:rgba(139,148,158,.1);color:var(--muted)">${t('badge.once')} &#10003;</span>`)
      : (s.enabled
          ? `<span class="badge badge-done">${t('badge.active')}</span>`
          : `<span class="badge" style="background:rgba(139,148,158,.1);color:var(--muted)">${t('badge.inactive')}</span>`)
    }</td>
    <td>
      <button class="btn btn-sm" onclick='openEditModal(${JSON.stringify(s)})' title="${t('common.edit')}">&#9998;</button>
      <button class="btn btn-sm btn-danger" onclick="deleteSerie('${s.id}','${escAttr(s.name)}')" title="${t('common.remove')}">&#128465;</button>
    </td>
  </tr>`;
  }).join('');
}

async function deleteSerie(id, name) {
  if (!confirm(t('confirm.delete_series',{n:name}))) return;
  await fetch('/api/series/'+id, {method:'DELETE'});
  loadSeries(); loadStatus();
}

// ── Aufnahmen ─────────────────────────────────────────────
async function loadRecordings() {
  let recs = await fetch('/api/recordings').then(r => r.json());
  const f    = document.getElementById('rec-filter').value;
  const sort = document.getElementById('rec-sort')?.value || 'start_desc';
  if (f) recs = recs.filter(r => r.status === f);
  recs.sort((a, b) => {
    switch(sort) {
      case 'start_asc':  return (a.start_ts||0) - (b.start_ts||0);
      case 'serie_asc':  return (a.serie_name||'').localeCompare(b.serie_name||'', 'de');
      case 'serie_desc': return (b.serie_name||'').localeCompare(a.serie_name||'', 'de');
      case 'title_asc':  return (a.title||'').localeCompare(b.title||'', 'de');
      case 'title_desc': return (b.title||'').localeCompare(a.title||'', 'de');
      case 'status':     return (a.status||'').localeCompare(b.status||'', 'de');
      default:           return (b.start_ts||0) - (a.start_ts||0); // start_desc
    }
  });
  const tbody = document.getElementById('rec-tbody');
  if (!recs.length) { tbody.innerHTML='<tr><td colspan="7" class="empty">' + t('rec.empty') + '</td></tr>'; return; }
  const now = Date.now() / 1000;
  tbody.innerHTML = recs.map(rec => {
    const startStr = fmtTs(rec.start_ts);
    const endTs    = rec.stop_ts_eff || rec.stop_ts;
    const endStr   = fmtTs(endTs);
    // Fortschritt bei laufenden Aufnahmen
    let progressHtml = '';
    if (rec.status === 'recording' && rec.start_ts && endTs) {
      const total   = endTs - rec.start_ts;
      const elapsed = Math.max(0, now - rec.start_ts);
      const pct     = Math.min(100, (elapsed / total * 100)).toFixed(0);
      const remaining = Math.max(0, endTs - now);
      const remMin  = Math.ceil(remaining / 60);
      progressHtml = `
        <div class="rec-progress"><div class="rec-progress-bar" style="width:${pct}%"></div></div>
        <div style="font-size:.72rem;color:var(--muted);margin-top:2px">${t('rec.remaining',{n:remMin})}</div>`;
    }
    const prot = rec.protected
      ? `<button class="btn btn-sm" onclick="toggleProtect('${rec.id}',false)" title="${t('rec.unprotect_title')}">&#128274;</button>`
      : `<button class="btn btn-sm" onclick="toggleProtect('${rec.id}',true)" title="${t('rec.protect_title')}">&#128275;</button>`;
    return `<tr>
      <td>${escHtml(rec.serie_name||'')}</td>
      <td>${escHtml(rec.title)}${rec.protected?' &#128274;':''}${rec.file_missing?` <span style="color:var(--red);font-size:.72rem" title="${t('rec.file_missing_title')}">&#9888;</span>`:''}${rec.subtitle?'<br><span style="color:var(--muted);font-size:.8rem">'+escHtml(rec.subtitle)+'</span>':''}</td>
      <td style="font-size:.82rem">${startStr}</td>
      <td style="font-size:.82rem">${endStr}${progressHtml}</td>
      <td style="font-size:.78rem;color:var(--muted)">${rec.filesize ? (rec.filesize/1024/1024).toFixed(1)+' MB' : ''}</td>
      <td style="font-size:.82rem;color:var(--muted)">${rec.proxy_url ? rec.proxy_url.replace('http://','') : '—'}</td>
      <td>${statusBadge(rec.status)}</td>
      <td>
        <button class="btn btn-sm" onclick="showRecordingDetail('${rec.id}')" title="${t('rec.info_title')}" style="font-family:'JetBrains Mono',monospace;font-weight:700">i</button>
        ${prot}
        <button class="btn btn-sm btn-danger" onclick="deleteRec('${rec.id}')" title="${t('rec.delete_title')}">&#128465;</button>
      </td>
    </tr>`;
  }).join('');
}

async function playRecording(id) {
  const rec = await fetch('/api/recordings').then(r=>r.json()).then(recs=>recs.find(r=>r.id===id));
  if (!rec) return;
  // Stream-URL: Proxy streamt die fertige Datei direkt
  // Wir öffnen den e2proxy Stream-URL im Browser (webm-Profil für Browser-Kompatibilität)
  const proxyUrl = rec.proxy_url;
  const fp = rec.filepath || '';
  if (!proxyUrl || !fp) { showToast(t('toast.no_stream'), 'error'); return; }
  // Mini-Player Modal
  const overlay = document.createElement('div');
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:300;display:flex;flex-direction:column;align-items:center;justify-content:center';
  overlay.innerHTML = `
    <div style="width:min(900px,95vw);background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden">
      <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 16px;border-bottom:1px solid var(--border)">
        <span style="font-family:'JetBrains Mono',monospace;font-size:.82rem;color:var(--accent2)">${escHtml(rec.title)}</span>
        <div style="display:flex;gap:8px">
          <button onclick="document.getElementById('rec-player').requestFullscreen()" class="btn btn-sm">&#10138; ${t('player.fullscreen')}</button>
          <button onclick="this.closest('div[style*=fixed]').remove()" class="btn btn-sm btn-danger">&#10005; ${t('common.close')}</button>
        </div>
      </div>
      <video id="rec-player" controls autoplay playsinline
        style="width:100%;max-height:70vh;background:#000;display:block"
        src="${proxyUrl}/recording/stream?file=${encodeURIComponent(fp)}">
        ${t('player.no_video')}
      </video>
      <div style="padding:8px 16px;font-size:.75rem;color:var(--muted);font-family:'JetBrains Mono',monospace">
        ${escHtml(fp)}
      </div>
    </div>
  `;
  overlay.addEventListener('click', e => {
    if (e.target === overlay) { document.getElementById('rec-player')?.pause(); overlay.remove(); }
  });
  document.body.appendChild(overlay);
}

async function copyStreamUrl(id) {
  const rec = await fetch('/api/recordings').then(r=>r.json()).then(recs=>recs.find(r=>r.id===id));
  if (!rec || !rec.proxy_url || !rec.filepath) { showToast(t('toast.no_url'), 'error'); return; }
  const url = rec.proxy_url + '/recording/stream?file=' + encodeURIComponent(rec.filepath);
  try {
    await navigator.clipboard.writeText(url);
    showToast(t('toast.url_copied'), 'success');
  } catch(e) {
    prompt('Stream-URL:', url);
  }
}

async function clearFailedRecs() {
  if (!confirm(t('confirm.clear_failed'))) return;
  const recs = await fetch('/api/recordings').then(r => r.json());
  const toDelete = recs.filter(r =>
    r.status === 'failed' ||
    r.status === 'missed' ||
    r.status === 'unknown' ||
    r.status === 'skipped' ||
    (r.status === 'done' && r.file_missing === true)
  );
  for (const rec of toDelete) {
    await fetch('/api/recordings/' + rec.id, {method: 'DELETE'});
  }
  showToast(t('toast.entries_deleted',{n:toDelete.length}), 'success');
  loadRecordings();
}

async function rescueScan() {
  const btn = event?.target;
  if (btn) { btn.innerHTML = '&#128269; ' + t('rec.scanning'); btn.disabled = true; }
  const r = await fetch('/api/admin/rescan', {method:'POST', headers:{'Content-Type':'application/json'}}).then(r => r.json());
  // Warte kurz damit der Hintergrund-Thread fertig ist
  await new Promise(res => setTimeout(res, 2000));
  await loadRecordings();
  showToast(t('toast.rescan',{n:(r.msg || 'OK')}), 'success');
  if (btn) { btn.innerHTML = '&#128269; ' + t('rec.rescan'); btn.disabled = false; }
}

function infoRow(label, value) {
  return `<div style="background:var(--surface2);border-radius:5px;padding:8px 10px">
    <div style="font-family:'JetBrains Mono',monospace;font-size:.68rem;color:var(--muted);margin-bottom:3px">${label}</div>
    <div style="font-size:.82rem">${value}</div>
  </div>`;
}

async function showRecordingDetail(id) {
  const detail = await fetch('/api/recordings/' + id + '/detail').then(r => r.json());
  if (detail.error) { showToast(t('toast.error',{n:detail.error}), 'error'); return; }
  const fp      = detail.filepath || '';
  const exists  = detail.file_exists;
  const streamUrl = detail.stream_url || '';
  const mb      = detail.filesize ? (detail.filesize/1024/1024).toFixed(1) + ' MB' : '—';
  const startStr = detail.start_ts ? new Date(detail.start_ts*1000).toLocaleString(LC()) : '—';
  const endStr   = detail.stop_ts  ? new Date(detail.stop_ts*1000).toLocaleString(LC())  : '—';
  const source   = detail.source   || 'epg-scheduler';
  const receiver = detail.receiver || '—';
  const statusColors = {done:'var(--green)',failed:'var(--red)',missed:'var(--yellow)',recording:'var(--accent2)',scheduled:'var(--blue)'};
  const statusColor  = statusColors[detail.status] || 'var(--muted)';
  const overlay = document.createElement('div');
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.8);z-index:300;display:flex;align-items:center;justify-content:center';
  overlay.innerHTML = `
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;width:min(640px,94vw);max-height:90vh;overflow-y:auto">
      <div style="padding:14px 18px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between">
        <div>
          <div style="font-family:'JetBrains Mono',monospace;font-size:.82rem;font-weight:700;color:var(--accent2)">${escHtml(detail.serie_name||'')}</div>
          <div style="font-size:1rem;font-weight:600;margin-top:2px">${escHtml(detail.title||'')}</div>
          ${detail.subtitle ? `<div style="color:var(--blue);font-size:.8rem;margin-top:1px">${escHtml(detail.subtitle)}</div>` : ''}
        </div>
        <button onclick="this.closest('div[style*=fixed]').remove()" class="btn btn-sm">&#10005;</button>
      </div>
      <div style="padding:14px 18px;display:grid;grid-template-columns:1fr 1fr;gap:10px">
        ${infoRow(t('detail.status'), `<span style="color:${statusColor};font-weight:600">${escHtml(detail.status||'')}</span>`)}
        ${infoRow(t('detail.source'), escHtml(source))}
        ${infoRow(t('detail.start'), escHtml(startStr))}
        ${infoRow(t('detail.end'), escHtml(endStr))}
        ${infoRow(t('detail.channel'), escHtml(detail.channel_name||'—'))}
        ${infoRow(t('detail.proxy'), escHtml((detail.proxy_url||'—').replace(/https?:\/\//, '')))}
        ${infoRow(t('detail.tuner'), escHtml(receiver) + (detail.shared_tuner ? ` <span style="color:var(--yellow);font-size:.72rem">(${t('detail.shared')})</span>` : ''))}
        ${infoRow(t('detail.filesize'), mb)}
        ${infoRow(t('detail.file'), exists ? `<span style="color:var(--green)">&#10003; ${t('detail.exists')}</span>` : `<span style="color:var(--red)">&#10005; ${t('detail.missing')}</span>`)}
      </div>
      <div style="padding:0 18px 14px">
        <div style="font-family:'JetBrains Mono',monospace;font-size:.7rem;color:var(--muted);margin-bottom:4px">${t('detail.filepath')}</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:.73rem;color:${fp?'var(--text)':'var(--red)'};background:var(--bg);padding:8px 10px;border-radius:5px;border:1px solid var(--border);word-break:break-all">
          ${fp ? escHtml(fp) : t('detail.no_path')}
        </div>
      </div>
      ${detail.desc ? `<div style="padding:0 18px 14px;font-size:.8rem;color:var(--muted);line-height:1.5">${escHtml(detail.desc)}</div>` : ''}
      <div style="padding:14px 18px;border-top:1px solid var(--border);display:flex;gap:8px;justify-content:flex-end">
        <button onclick="this.closest('div[style*=fixed]').remove()" class="btn">${t('common.close')}</button>
      </div>
    </div>
  `;
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

function playFromDetail(streamUrl, title) {
  document.querySelector('div[style*="z-index:300"]')?.remove();
  const overlay = document.createElement('div');
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.9);z-index:400;display:flex;flex-direction:column;align-items:center;justify-content:center';
  overlay.innerHTML = `
    <div style="width:min(960px,96vw);background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden">
      <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 16px;border-bottom:1px solid var(--border)">
        <span style="font-family:'JetBrains Mono',monospace;font-size:.8rem;color:var(--accent2)">${escHtml(title)}</span>
        <div style="display:flex;gap:6px">
          <button onclick="document.getElementById('rec-player').requestFullscreen()" class="btn btn-sm">&#10138;</button>
          <button onclick="document.getElementById('rec-player').pause();this.closest('div[style*=fixed]').remove()" class="btn btn-sm btn-danger">&#10005;</button>
        </div>
      </div>
      <video id="rec-player" controls autoplay playsinline style="width:100%;max-height:72vh;background:#000;display:block" src="${escHtml(streamUrl)}"></video>
    </div>
  `;
  overlay.addEventListener('click', e => { if (e.target === overlay) { document.getElementById('rec-player')?.pause(); overlay.remove(); } });
  document.body.appendChild(overlay);
}

async function playRecording(id) { await showRecordingDetail(id); }

async function copyUrl(url) {
  try { await navigator.clipboard.writeText(url); showToast(t('toast.url_copied'), 'success'); }
  catch(e) { prompt('Stream-URL:', url); }
}

async function copyStreamUrl(id) {
  const detail = await fetch('/api/recordings/' + id + '/detail').then(r => r.json());
  if (detail.stream_url) copyUrl(detail.stream_url);
  else showToast(t('toast.no_stream_url'), 'error');
}

async function toggleProtect(id, val) {
  await fetch('/api/recordings/'+id+'/keep', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({protected:val})});
  loadRecordings();
}
async function deleteRec(id) {
  if (!confirm(t('confirm.delete_rec'))) return;
  await fetch('/api/recordings/'+id, {method:'DELETE'});
  loadRecordings(); loadStatus();
}

// ── Proxies ───────────────────────────────────────────────
async function loadProxies() {
  const proxies = await fetch('/api/proxies').then(r => r.json());
  const el = document.getElementById('proxies-list');
  if (!proxies.length) { el.innerHTML='<div class="empty">' + t('proxy.empty_cfg') + '</div>'; return; }
  el.innerHTML = proxies.map(p => `
    <div class="proxy-card ${p.online?'online':'offline'}">
      <div>
        <div class="proxy-name">
          <span class="dot ${p.online?'dot-green':'dot-red'}" style="margin-right:6px"></span>
          ${escHtml(p.name)}
        </div>
        <div class="proxy-url">${escHtml(p.url)}</div>
        ${p.online ? `
          <div class="tuner-bar">
            ${p.receivers.map(r => `<div class="tuner-dot ${r.busy?'busy':'free'}" title="${escHtml(r.name)}: ${r.busy?t('proxy.busy',{n:escHtml(r.channel)}):t('proxy.free')}"></div>`).join('')}
            <span style="font-size:.75rem;color:var(--muted);margin-left:6px">${t('proxy.free_total',{a:p.free,b:p.total})}</span>
          </div>` : `<div style="font-size:.75rem;color:var(--red);margin-top:4px">${t('proxy.offline')}</div>`}
      </div>
      <div style="display:flex;gap:8px">
        <button class="btn btn-sm" onclick="toggleProxy('${escAttr(p.url)}', ${!p.enabled})">${p.enabled?t('proxy.disable'):t('proxy.enable')}</button>
        <button class="btn btn-sm btn-danger" onclick="removeProxy('${escAttr(p.url)}')">&#128465;</button>
      </div>
    </div>
  `).join('');
}

async function addProxyManual() {
  const url  = document.getElementById('proxy-url-input').value.trim().replace(/\/+$/, '');
  const name = document.getElementById('proxy-name-input').value.trim();
  if (!url) return;
  await fetch('/api/proxies', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({url,name})});
  document.getElementById('proxy-url-input').value = '';
  document.getElementById('proxy-name-input').value = '';
  loadProxies(); loadStatus();
}

async function toggleProxy(url, enabled) {
  await fetch('/api/proxies', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({url, enabled})});
  loadProxies();
}
async function removeProxy(url) {
  if (!confirm(t('confirm.remove_proxy'))) return;
  await fetch('/api/proxies/remove', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({url})});
  loadProxies();
}
async function runDiscovery() {
  document.getElementById('discovery-btn').textContent = t('proxy.searching');
  await fetch('/api/discover', {method:'POST'});
  setTimeout(() => { loadProxies(); document.getElementById('discovery-btn').textContent = t('proxy.ssdp_discovery'); }, 6000);
}

// ── Config ────────────────────────────────────────────────
async function loadConfig() {
  const c = await fetch('/api/config').then(r => r.json());
  document.getElementById('cfg-recdir').value        = c.recordings_subdir || '';
  document.getElementById('cfg-pre').value           = c.pre_buffer_sec ?? 30;
  document.getElementById('cfg-post').value          = c.post_buffer_sec ?? 60;
  document.getElementById('cfg-cleanup').value       = c.cleanup_trigger || 'on_new';
  document.getElementById('cfg-cleanup-hour').value  = c.cleanup_hour ?? 4;
  document.getElementById('cfg-epg-interval').value  = c.epg_scan_interval ?? 3600;
  document.getElementById('cfg-epg-lookahead').value = c.epg_lookahead_hours ?? 72;
  document.getElementById('cfg-tmdb-key').value      = c.tmdb_api_key || '';
  document.getElementById('cfg-tmdb-lang').value     = c.tmdb_language || 'de-DE';
  updateCleanupUi();
  // Profile aus Proxy laden
  await loadProxyProfiles(c.stream_profile || 'remux-ac3');
  // Proxies in Settings anzeigen
  loadSettingsProxies();
}

async function loadProxyProfiles(currentProfile) {
  const sel = document.getElementById('cfg-profile');
  if (!sel) return;
  try {
    // Ersten verbundenen Proxy finden
    const proxies = await fetch('/api/proxies').then(r => r.json());
    const online  = proxies.find(p => p.online && p.enabled);
    if (!online) return;
    const cfg = await fetch(online.url + '/api/config').then(r => r.json());
    const profiles = cfg.transcode_profiles || {};
    sel.innerHTML = Object.entries(profiles).map(([id, tp]) =>
      `<option value="${id}" ${id === currentProfile ? 'selected' : ''}>${tp.label || id}</option>`
    ).join('');
    // Fallback wenn leer
    if (!sel.options.length) {
      sel.innerHTML = '<option value="remux-ac3">remux-ac3</option>';
    }
  } catch(e) {
    sel.innerHTML = `<option value="${currentProfile}" selected>${currentProfile}</option>`;
  }
}

async function loadSettingsProxies() {
  const el = document.getElementById('settings-proxies-list');
  if (!el) return;
  const proxies = await fetch('/api/proxies').then(r => r.json());
  if (!proxies.length) {
    el.innerHTML = '<div class="empty" style="padding:10px">' + t('proxy.empty_cfg') + '</div>';
    return;
  }
  el.innerHTML = proxies.map(p => `
    <div class="proxy-card ${p.online?'online':'offline'}" style="margin-bottom:6px">
      <div>
        <div class="proxy-name">
          <span class="dot ${p.online?'dot-green':'dot-red'}" style="margin-right:5px"></span>
          ${escHtml(p.name)}
          ${p.online ? `<span style="color:var(--muted);font-size:.7rem;margin-left:6px">${t('proxy.free_short',{a:p.free,b:p.total})}</span>` : ''}
        </div>
        <div class="proxy-url">${escHtml(p.url)}</div>
      </div>
      <div style="display:flex;gap:6px">
        <button class="btn btn-sm" onclick="toggleProxy('${escAttr(p.url)}',${!p.enabled})">${p.enabled?t('proxy.off'):t('proxy.on')}</button>
        <button class="btn btn-sm btn-danger" onclick="removeProxySettings('${escAttr(p.url)}')">&#128465;</button>
      </div>
    </div>
  `).join('');
}

async function addProxyFromSettings() {
  const url  = document.getElementById('settings-proxy-url').value.trim().replace(/\/+$/, '');
  const name = document.getElementById('settings-proxy-name').value.trim();
  if (!url) return;
  await fetch('/api/proxies', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({url, name})});
  document.getElementById('settings-proxy-url').value = '';
  document.getElementById('settings-proxy-name').value = '';
  loadSettingsProxies();
  showToast(t('toast.proxy_added'), 'success');
}

async function removeProxySettings(url) {
  await fetch('/api/proxies/remove', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({url})});
  loadSettingsProxies();
}
function updateCleanupUi() {
  const t = document.getElementById('cfg-cleanup').value;
  document.getElementById('cleanup-hour-wrap').style.opacity = t==='daily'?'1':'0.4';
  document.getElementById('cleanup-hour-wrap').style.pointerEvents = t==='daily'?'auto':'none';
}
async function saveAllConfig() {
  const body = {
    recordings_subdir:   document.getElementById('cfg-recdir').value,
    log_retention_days:  +(document.getElementById('cfg-log-retention')?.value || 30),
    stream_profile:      document.getElementById('cfg-profile').value,
    pre_buffer_sec:      +document.getElementById('cfg-pre').value,
    post_buffer_sec:     +document.getElementById('cfg-post').value,
    cleanup_trigger:     document.getElementById('cfg-cleanup').value,
    cleanup_hour:        +document.getElementById('cfg-cleanup-hour').value,
    epg_scan_interval:   +document.getElementById('cfg-epg-interval').value,
    epg_lookahead_hours: +document.getElementById('cfg-epg-lookahead').value,
    tmdb_api_key:        document.getElementById('cfg-tmdb-key').value,
    tmdb_language:       document.getElementById('cfg-tmdb-lang').value,
  };
  await fetch('/api/config', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
  showToast(t('toast.saved'), 'success');
}
async function loadTunerHistory() {
  const el = document.getElementById('tuner-history-container');
  if (!el) return;
  const history = await fetch('/api/tuner/history').then(r => r.json());
  if (!history.length) {
    el.innerHTML = '<div class="empty" style="padding:16px">' + t('table.empty') + '</div>';
    return;
  }
  el.innerHTML = history.map(e => {
    const ts = new Date(e.ts).toLocaleString(LC(), {day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});
    const ok = e.chosen && !e.error;
    const color = ok ? 'var(--green)' : 'var(--red)';
    const icon  = ok ? '&#10003;' : '&#10005;';
    // Receiver-Details
    const recvDetails = (e.proxies||[]).map(p => {
      if (!p.online) return `<span style="color:var(--red)">${escHtml(p.url)}: ${t('proxy.offline_l')}</span>`;
      const recvs = (p.receivers||[]).map(r =>
        `${escHtml(r.name||r.id)}: ${r.busy
          ? `<span style="color:var(--yellow)">${t('proxy.busy',{n:escHtml(r.channel||'')})}</span>`
          : `<span style="color:var(--green)">${t('proxy.free')}</span>`}`
      ).join(', ');
      return `${escHtml(p.url.replace(/https?:\/\//, ''))} [${p.free}/${p.total} ${t('proxy.free')}${recvs ? ' — ' + recvs : ''}]`;
    }).join('<br>');
    return `<div style="padding:8px 10px;border-bottom:1px solid var(--border);font-size:.75rem">
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="font-family:'JetBrains Mono',monospace;font-weight:600">${escHtml(e.title||'?')}</span>
        <span style="color:var(--muted)">${ts}</span>
      </div>
      <div style="color:${color};font-family:'JetBrains Mono',monospace;font-size:.7rem">
        ${icon} ${e.error ? escHtml(e.error) : t('tuner.recorded_via',{n:escHtml((e.chosen||'').replace(/https?:\/\//, ''))})}
      </div>
      ${recvDetails ? `<div style="color:var(--muted);font-size:.7rem;margin-top:2px">${recvDetails}</div>` : ''}
    </div>`;
  }).join('');
}

async function toggleApiLogging() {
  const c = await fetch('/api/config').then(r => r.json());
  const current = c.api_call_logging || false;
  await fetch('/api/config', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({api_call_logging: !current})});
  const btn = document.getElementById('api-log-btn');
  if (btn) btn.textContent = !current ? t('proxy.off') : t('proxy.on');
  if (btn) btn.style.borderColor = !current ? 'var(--yellow)' : '';
  if (btn) btn.style.color = !current ? 'var(--yellow)' : '';
  showToast(t('toast.api_log',{n: !current ? t('common.on_caps') : t('common.off_caps')}), !current ? '' : 'success');
  if (!current) showToast(t('toast.api_log_hint'), '');
}

async function setLogLevel(level) {
  const r = await fetch('/api/log/level', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({level})
  }).then(r => r.json());
  if (r.ok) {
    showToast(t('toast.log_level',{n:level}), 'success');
    const el = document.getElementById('log-level-status');
    if (el) el.textContent = t('settings.current',{n:level});
    // Logs sofort aktualisieren
    setTimeout(loadLogs, 500);
  }
}

async function triggerCleanupNow() {
  await fetch('/api/cleanup', {method:'POST'});
  showToast(t('toast.cleanup_started'), 'success');
}

async function deduplicateDb() {
  const r = await fetch('/api/admin/deduplicate', {method:'POST'}).then(r => r.json());
  showToast(t('toast.dups_removed',{n:r.removed}), r.removed > 0 ? 'success' : '');
  loadRecordings(); loadStatus();
}

// ── Logs ──────────────────────────────────────────────────
async function loadLogs() {
  const lvl = document.getElementById('log-level').value;
  const entries = await fetch('/api/logs?level='+lvl).then(r => r.json());
  const c = document.getElementById('log-container');
  c.innerHTML = entries.length
    ? [...entries].reverse().map(e => `<div class="log-entry"><span class="ts">${e.ts}</span><span class="level-${e.level}">${e.level}</span> ${escHtml(e.msg)}</div>`).join('')
    : `<div style="color:var(--muted)">${t('logs.empty')}</div>`;
  // Log-Dateien auf Disk anzeigen
  try {
    const files = await fetch('/api/logs/files').then(r => r.json());
    const el = document.getElementById('log-files-list');
    if (el) {
      if (!files.length) {
        el.innerHTML = `<span style="color:var(--muted)">${t('logs.no_files')}</span>`;
      } else {
        el.innerHTML = files.map(f => {
          const kb = (f.size / 1024).toFixed(0);
          const date = f.modified.split('T')[0];
          return `<div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid var(--border)">
            <span>${escHtml(f.name)}</span>
            <span style="color:var(--muted)">${kb} KB &nbsp; ${date}</span>
          </div>`;
        }).join('');
      }
    }
  } catch(e) {}
}

// ── Modal ─────────────────────────────────────────────────
async function openAddModal(defaultKind) {
  const kind = (defaultKind === 'movie') ? 'movie' : 'series';
  document.getElementById('modal-title').textContent = t(kind === 'movie' ? 'modal.add_movie' : 'modal.add_series');
  ['edit-id','s-name','s-regex','tmdb-query'].forEach(id => document.getElementById(id).value='');
  document.getElementById('s-keep').value='0';
  document.getElementById('s-enabled').value='true';
  document.getElementById('s-kind').value=kind;
  document.getElementById('s-year').value='';
  document.getElementById('s-pre').value=0;
  document.getElementById('s-post').value=0;
  document.getElementById('tmdb-results').style.display='none';
  document.getElementById('tmdb-results').innerHTML='';
  document.getElementById('regex-preview').style.display='none';
  _selectedTmdbId=null; _selectedTmdbPoster='';
  clearChannel(); _channels=[];
  await loadChannelsForModal();
  document.getElementById('modal').classList.add('open');
}

async function openEditModal(s) {
  document.getElementById('modal-title').textContent = t(s.kind === 'movie' ? 'modal.edit_movie' : 'modal.edit_series');
  document.getElementById('edit-id').value   = s.id;
  document.getElementById('s-name').value    = s.name;
  document.getElementById('s-regex').value   = s.regex_pattern||'';
  document.getElementById('s-keep').value    = s.keep_last||0;
  document.getElementById('s-enabled').value = s.enabled?'true':'false';
  document.getElementById('s-kind').value    = s.kind || 'series';
  document.getElementById('s-year').value    = s.year || '';
  document.getElementById('s-pre').value     = s.pre_offset_sec || 0;
  document.getElementById('s-post').value    = s.post_offset_sec || 0;
  document.getElementById('tmdb-query').value= s.name;
  document.getElementById('tmdb-results').style.display='none';
  document.getElementById('regex-preview').style.display='none';
  _selectedTmdbId=s.tmdb_id||null; _selectedTmdbPoster=s.tmdb_poster||'';
  _selRef=s.channel_ref; _selName=s.channel_name;
  document.getElementById('s-ch-ref').value=s.channel_ref;
  document.getElementById('s-ch-name').value=s.channel_name;
  updateRegexPreview();
  await loadChannelsForModal();
  document.getElementById('modal').classList.add('open');
}

function closeModal() { document.getElementById('modal').classList.remove('open'); }

async function loadChannelsForModal() {
  const sel = document.getElementById('s-ch-select');
  if (!_channels.length) {
    try { _channels = await fetch('/api/channels').then(r => r.json()); } catch(e) { _channels=[]; }
  }
  const current = document.getElementById('s-ch-ref').value;
  const groups = {};
  for (const c of _channels) {
    const g = c.group||'Alle';
    if (!groups[g]) groups[g]=[];
    groups[g].push(c);
  }
  sel.innerHTML = '<option value="">' + t('modal.choose_channel') + '</option>';
  // Normalisierung: Refs können _ oder : als Separator haben, Endung variabel
  const normRef = r => (r||'').replace(/[:_]+$/,'').replace(/:/g,'_').toLowerCase();
  const currentNorm = normRef(current);
  for (const [group, channels] of Object.entries(groups)) {
    const og = document.createElement('optgroup');
    og.label = group;
    for (const c of channels) {
      const opt = document.createElement('option');
      opt.value=c.ref; opt.dataset.name=c.name; opt.textContent=c.name;
      if (normRef(c.ref) === currentNorm) opt.selected = true;
      og.appendChild(opt);
    }
    sel.appendChild(og);
  }
  // Fallback: wenn nichts gematcht hat aber current gesetzt ist → manuelle Option
  if (current && !sel.value) {
    const opt = document.createElement('option');
    opt.value = current;
    opt.textContent = document.getElementById('s-ch-name').value + ' ' + t('modal.saved');
    opt.selected = true;
    sel.insertBefore(opt, sel.options[1]);
  }
}
function onChannelSelect(sel) {
  const opt = sel.options[sel.selectedIndex];
  _selRef=opt.value; _selName=opt.dataset.name||opt.textContent;
  document.getElementById('s-ch-ref').value=_selRef;
  document.getElementById('s-ch-name').value=_selName;
}
function clearChannel() {
  _selRef=''; _selName='';
  document.getElementById('s-ch-ref').value='';
  document.getElementById('s-ch-name').value='';
  const sel=document.getElementById('s-ch-select');
  if(sel) sel.value='';
}

async function doTmdbSearch() {
  const q = document.getElementById('tmdb-query').value.trim();
  if (!q) return;
  const el = document.getElementById('tmdb-results');
  el.style.display='block';
  el.innerHTML=`<div style="color:var(--muted);padding:8px">${t('proxy.searching')}</div>`;
  const data = await fetch('/api/tmdb/search?q='+encodeURIComponent(q)).then(r=>r.json());
  if (data.error) { el.innerHTML=`<div style="color:var(--red);padding:8px">${escHtml(data.error)}</div>`; return; }
  const results = data.results||[];
  if (!results.length) { el.innerHTML=`<div style="color:var(--muted);padding:8px">${t('tmdb.no_results')}</div>`; return; }
  el.innerHTML = results.map(r => `
    <div class="tmdb-item" id="tmdb-${r.tmdb_id}" onclick="selectTmdb(${r.tmdb_id},'${escAttr(r.name)}','${escAttr(r.regex_suggestion)}','${escAttr(r.poster_large||'')}')">
      ${r.poster?`<img class="tmdb-poster" src="${r.poster}" alt="">`:'<div class="tmdb-poster"></div>'}
      <div class="tmdb-info">
        <div class="tname">${escHtml(r.name)}</div>
        <div class="tmeta">${escHtml(r.original_name)} &#183; ${r.year} &#183; ${r.kind==='tv'?t('badge.series'):t('badge.movie')}</div>
        ${r.aliases.length?`<div class="tmeta">${t('tmdb.de')} ${escHtml(r.aliases.join(', '))}</div>`:''}
        ${r.regex_suggestion?`<div class="tregex">${t('tmdb.regex')} ${escHtml(r.regex_suggestion)}</div>`:''}
      </div>
    </div>`).join('');
}

function selectTmdb(id, name, regex, poster) {
  _selectedTmdbId=id; _selectedTmdbPoster=poster;
  document.querySelectorAll('.tmdb-item').forEach(el=>el.classList.remove('selected'));
  document.getElementById('tmdb-'+id)?.classList.add('selected');
  if (!document.getElementById('s-name').value) document.getElementById('s-name').value=name;
  if (regex) { document.getElementById('s-regex').value=regex; updateRegexPreview(); }
}
function updateRegexPreview() {
  const val = document.getElementById('s-regex').value.trim();
  const prev = document.getElementById('regex-preview');
  if (val) { prev.style.display='block'; prev.textContent=val; } else prev.style.display='none';
}

async function saveSerie() {
  const id    = document.getElementById('edit-id').value;
  const name  = document.getElementById('s-name').value.trim();
  const ref   = document.getElementById('s-ch-ref').value;
  const cname = document.getElementById('s-ch-name').value;
  const regex = document.getElementById('s-regex').value.trim();
  if (!name||!ref) { showToast(t('toast.required'), 'error'); return; }
  // Duplikat-Check (nur bei neuer Serie, nicht beim Bearbeiten)
  if (!id) {
    const existing = await fetch('/api/series').then(r=>r.json());
    const dup = existing.find(s => s.channel_ref === ref && s.name.toLowerCase() === name.toLowerCase());
    if (dup) {
      showToast(t('toast.dup_series',{n:name}), 'error');
      return;
    }
  }
  const body = {
    name, channel_ref:ref, channel_name:cname,
    keep_last:       +document.getElementById('s-keep').value,
    enabled:         document.getElementById('s-enabled').value==='true',
    regex_pattern:   regex||name,
    tmdb_id:         _selectedTmdbId,
    tmdb_poster:     _selectedTmdbPoster,
    kind:            document.getElementById('s-kind').value || 'series',
    year:            +document.getElementById('s-year').value || null,
    pre_offset_sec:  +document.getElementById('s-pre').value || 0,
    post_offset_sec: +document.getElementById('s-post').value || 0,
  };
  if (id) await fetch('/api/series/'+id, {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
  else     await fetch('/api/series',    {method:'POST',headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
  closeModal(); loadSeries(); loadStatus();
  fetch('/api/scan', {method:'POST'});
}

// ── Helpers ───────────────────────────────────────────────
function _chNameById(id) {
  // Sucht Sendernamen aus gecachten Favoriten
  if (!id) return '';
  const norm = id.replace(/[:_]+$/,'');
  const ch = _channels.find(c => {
    const r = (c.ref||'').replace(/[:_]+$/,'').replace(/:/g,'_');
    return r === norm || c.ref === id;
  });
  return ch ? ch.name : '';
}

function escHtml(s) { return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function escAttr(s) { return String(s||'').replace(/\\\\/g,'\\\\\\\\').replace(/'/g,"\\\\'"); }
function fmtTs(ts) {
  if (!ts) return '&#8212;';
  const d = new Date(ts*1000);
  return d.toLocaleDateString(LC(),{day:'2-digit',month:'2-digit'}) + ' ' +
         d.toLocaleTimeString(LC(),{hour:'2-digit',minute:'2-digit'});
}
function statusBadge(s) {
  const m = {
    scheduled: ['badge-scheduled', t('status.scheduled')],
    recording:  ['badge-recording', '&#11044; ' + t('status.recording')],
    done:       ['badge-done',      t('status.done')],
    failed:     ['badge-failed',    t('status.failed')],
    skipped:    ['badge-skipped',   t('status.skipped')],
    missed:     ['badge-missed',    t('status.missed')],
    unknown:    ['badge-unknown',   t('status.unknown')],
  };
  const [cls,lbl] = m[s]||['',''+s];
  return `<span class="badge ${cls}">${lbl}</span>`;
}



// Safe init - catch any startup errors
(async () => {
  try { await loadStatus(); } catch(e) { console.warn('loadStatus:', e); }
  try { await refreshChannels(); } catch(e) { console.warn('refreshChannels:', e); }
  try { await loadPlan(); }   catch(e) { console.warn('loadPlan:', e); }
})();
setInterval(loadStatus, 20000);
setInterval(() => {
  const active = document.querySelector('.tab.active')?.id;
  if (active==='tab-plan')       renderPlan();
  if (active==='tab-recordings') loadRecordings();
}, 30000);
"""

# ── i18n (client-side EN/DE) ───────────────────────────────────────────────
# Plain-text values only — NO unescaped double quotes inside dict values.
# t(key, vars) supports {n}-style placeholders. setLang() does a full reload.

I18N_JS = r"""<script>
(function(){
  var DICT = {
    en: {
      "nav.overview":"overview","nav.series":"series","nav.movies":"movies","nav.recordings":"recordings","nav.settings":"settings","nav.help":"Help",
      "hdr.proxy_connected":"{n} proxy connected","hdr.no_proxy":"no proxy — settings",
      "plan.title":"Recording Plan","plan.scan":"EPG Scan","plan.loading":"Loading...",
      "plan.legend_rec":"Will be recorded","plan.legend_live":"Now live","plan.legend_other":"Other program",
      "plan.empty_noepg":"No EPG data — click EPG Scan","plan.empty_window":"No programs in time window","plan.no_epg":"No EPG data yet",
      "tt.no_title":"(no title)","tt.no_desc":"No description available","tt.recording":"recording ({n})","tt.click_record":"Click to record","tt.skip":"Skip this recording",
      "qr.movie":"Movie","qr.series":"Series","qr.rec_movie":"Record movie","qr.movie_note":"Imported as a movie into the Plex Movies library",
      "qr.this_episode":"Only this episode","qr.once_note":"One-time recording of this single episode",
      "qr.all_episodes":"ALL EPISODES — including future ones","qr.pattern":"Search pattern (Regex)","qr.full_title":"Full title:","qr.shorten":"Shorten for more matches: e.g.","qr.rec_all":"Record all episodes",
      "toast.dup_planned":"Already exists — recording scheduled","toast.movie_planned":"Movie recording scheduled","toast.once_planned":"One-time recording scheduled","toast.series_created":"Series set up",
      "toast.error":"Error: {n}","toast.saved":"Saved","toast.proxy_added":"Proxy added","toast.cleanup_started":"Cleanup started",
      "toast.dups_removed":"{n} duplicates removed","toast.entries_deleted":"{n} entries deleted","toast.rescan":"Rescan: {n}",
      "toast.url_copied":"URL copied","toast.no_stream":"No stream available","toast.no_url":"No URL available","toast.no_stream_url":"No stream URL",
      "toast.required":"Name and channel are required!","toast.dup_series":"Series {n} already exists on this channel!",
      "toast.api_log":"API logging: {n}","toast.api_log_hint":"Set log level to DEBUG for full details","toast.log_level":"Log level: {n}",
      "confirm.delete_series":"Remove series {n}? Already recorded files are kept.","confirm.clear_failed":"Remove all error entries from the list? Recorded files are kept.","confirm.delete_rec":"Remove entry from the list? The recorded file is kept.","confirm.remove_proxy":"Remove proxy from the list?",
      "status.scheduled":"Scheduled","status.recording":"Recording","status.done":"Done","status.failed":"Failed","status.skipped":"Skipped","status.missed":"Missed","status.unknown":"?",
      "filter.all":"All",
      "sort.date_desc":"↓ Date","sort.date_asc":"↑ Date","sort.series_asc":"↑ Series","sort.series_desc":"↓ Series","sort.title_asc":"↑ Title","sort.title_desc":"↓ Title","sort.status":"Status",
      "th.name":"Name","th.type":"Type","th.channel":"Channel","th.regex":"Regex","th.keep":"Keep","th.status":"Status","th.action":"Action","th.series":"Series","th.title":"Title","th.start":"Start","th.end":"End","th.mb":"MB","th.proxy":"Proxy",
      "series.title":"Series","movies.title":"Movies","common.new":"+ New","table.empty":"No entries yet",
      "badge.movie":"Movie","badge.series":"Series","badge.once":"One-time","badge.active":"Active","badge.inactive":"Inactive",
      "common.all":"all","common.edit":"Edit","common.remove":"Remove","common.close":"Close","common.name":"Name","common.add":"Add","common.save":"Save","common.search":"Search","common.yes":"Yes","common.no":"No","common.days":"days","common.on_caps":"ON","common.off_caps":"OFF",
      "rec.title":"Recordings","rec.refresh":"Refresh","rec.refresh_title":"Refresh","rec.clear_failed":"Clear errors","rec.clear_failed_title":"Delete all error recordings","rec.rescan":"Rescan","rec.rescan_title":"Fetch status of all recordings from proxy","rec.scanning":"Scanning...",
      "rec.empty":"No recordings","rec.remaining":"~{n} min left","rec.info_title":"Information","rec.protect_title":"Protect","rec.unprotect_title":"Remove protection","rec.delete_title":"Delete entry (file is kept)","rec.file_missing_title":"File not found","rec.sort_title":"Sorting",
      "player.fullscreen":"Fullscreen","player.no_video":"Your browser does not support video.",
      "detail.status":"Status","detail.source":"Source","detail.start":"Start","detail.end":"End","detail.channel":"Channel","detail.proxy":"Proxy","detail.tuner":"Tuner","detail.shared":"shared","detail.filesize":"File size","detail.file":"File","detail.exists":"present","detail.missing":"missing","detail.filepath":"FILE PATH","detail.no_path":"no path",
      "proxy.empty_cfg":"No proxies configured","proxy.busy":"busy ({n})","proxy.free":"free","proxy.free_total":"{a} free / {b} total","proxy.offline":"Not reachable","proxy.disable":"Disable","proxy.enable":"Enable","proxy.off":"Off","proxy.on":"On","proxy.free_short":"{a}/{b} free","proxy.searching":"Searching...","proxy.ssdp_discovery":"SSDP discovery","proxy.offline_l":"offline",
      "tuner.recorded_via":"Recorded via {n}",
      "settings.appearance":"Appearance","settings.theme":"Theme","settings.theme_desc":"Light or dark — like in e2proxy","settings.dark":"Dark","settings.light":"Light","settings.language":"Language","settings.language_desc":"Interface language — like in e2proxy",
      "settings.proxies":"Proxies","settings.proxy_url":"Proxy URL","settings.recordings":"Recordings","settings.subdir":"Recording subfolder (optional)","settings.subdir_ph":"empty = e2proxy default path","settings.subdir_help":"Empty = e2proxy writes to its own path. Optional: subfolder relative to it, e.g. <code>Series</code>",
      "settings.profile":"Stream profile","settings.profile_help":"Loaded from proxy","settings.pre_buffer":"Pre-buffer (sec)","settings.post_buffer":"Post-buffer (sec)",
      "settings.cleanup":"Cleanup strategy","settings.trigger":"Trigger","settings.trig_onnew":"On new recording","settings.trig_daily":"Daily","settings.trig_never":"Never","settings.hour":"Time","settings.cleanup_now":"Keep-Last cleanup","settings.dedup":"Remove duplicates",
      "settings.epg_tmdb":"EPG & TMDB","settings.scan_interval":"Scan interval (sec)","settings.lookahead":"Preview (hours)","settings.tmdb_key":"TMDB API key","settings.tmdb_lang":"TMDB language",
      "settings.logs":"Logs","settings.log_files":"LOG FILES","settings.tuner_history":"Tuner history","settings.log_level":"Log level","settings.log_level_desc":"DEBUG shows all internal operations — ideal for troubleshooting. Set back to INFO after diagnosis.","settings.log_retention":"Keep log files:","settings.log_path_help":"Logs are in <code>/data/logs/</code> — rotated daily","settings.api_log":"API call logging","settings.api_log_desc":"Logs every request/response to e2proxy — enable only for troubleshooting",
      "logs.empty":"No entries","logs.no_files":"No files yet (appear after midnight)","settings.current":"Current: {n}",
      "modal.tmdb":"TMDB search (optional)","modal.tmdb_ph":"Search series name...","modal.name":"Series name","modal.channel":"Channel","modal.choose_channel":"-- choose channel --","modal.regex":"Regex pattern","modal.regex_help":"e.g. <code>GNTM|Germany.s Next Topmodel</code> · <code>^Tatort</code>","modal.type":"Type","modal.type_series":"Series","modal.type_movie":"Movie","modal.active":"Active","modal.keep":"Keep Last (0=all)","modal.year":"Year (for movies)","modal.year_ph":"e.g. 2018","modal.pre":"Start recording earlier (sec)","modal.pre_help":"Positive = start earlier, negative = later","modal.post":"Run recording longer (sec)","modal.post_help":"Positive = run longer, negative = end earlier","modal.add_series":"Add series","modal.add_movie":"Add movie","modal.edit_series":"Edit series","modal.edit_movie":"Edit movie","modal.saved":"(saved)",
      "tmdb.no_results":"No results","tmdb.de":"DE:","tmdb.regex":"Regex:",
      "help.title":"Help & Changelog","help.back":"Back","help.subtitle":"Help & Changelog",
      "help.h_overview":"Overview","help.overview":"Shows all channels as a horizontal timeline. Blue blocks = scheduled recordings, green = live.<br><br><strong>Click a slot:</strong> Record once or schedule as a series. Slots without an EPG description are clickable too.<br><br><strong>Hover:</strong> Title, time, description. For scheduled recordings: a Skip button.<br><br><strong>EPG scan:</strong> Loads fresh data from e2proxy, runs automatically every hour.",
      "help.h_series":"Series & Movies","help.series":"<strong>Type:</strong> For each recording you choose Movie or Series. Affects the path/library in Plex (Movies vs. TV Shows) and the .nfo format.<br><br><strong>Mode:</strong> For series additionally only this episode or all episodes.<br><br><strong>Regex pattern</strong> determines which EPG titles are recorded (case-insensitive):<br><code>Die Geissens</code> all episodes &bull; <code>GNTM|Germany.s Next Topmodel</code> both spellings &bull; <code>^Tatort</code> only titles starting with Tatort<br><br><strong>Pre/Post offset (sec):</strong> Configurable per series. Some channels regularly start their shows 30s earlier or end later. Positive values = start earlier / run longer.<br><br><strong>Back-to-back:</strong> If a recording directly follows another on the same channel (gap below 60s), it starts 2 minutes earlier automatically — avoids time drift.<br><br><strong>Keep Last:</strong> Keeps only the last N recordings, older ones are deleted automatically (incl. .nfo). 0 = all.<br><br><strong>TMDB search:</strong> Provides the local title, aliases and a regex suggestion.<br><br><strong>Duplicate protection:</strong> Same name + channel cannot be created twice.",
      "help.h_recordings":"Recordings","help.recordings":"Sortable by date, series, title, status. Filterable by status.<br><br><strong>Status:</strong> Scheduled · <span style=color:var(--accent2)>Running</span> · <span style=color:var(--green)>Done</span> · <span style=color:var(--red)>Failed</span> · <span style=color:var(--yellow)>Missed</span> · Skipped<br><br><strong>Detail:</strong> File path, size, proxy, source (epg-scheduler / ui-quickrecord).<br><br><strong>Play:</strong> Play the recording in the browser (requires <code>/recording/stream</code> in e2proxy).<br><br><strong>Rescan:</strong> Checks proxy status + file system, updates missing files.<br><br><strong>Clear errors:</strong> Removes all entries with status Failed, Missed, Skipped and missing files.",
      "help.h_settings":"Settings","help.settings":"<strong>Proxies:</strong> SSDP discovery or manual URL. Multiple proxies possible — the one with the most free tuners is chosen automatically.<br><br><strong>Stream profile:</strong> Loaded from e2proxy. Recommended: <code>remux-ac3</code>.<br><br><strong>Pre/Post buffer:</strong> Seconds before/after EPG time for imprecise broadcast times.<br><br><strong>Cleanup trigger:</strong> On new recording is recommended.<br><br><strong>TMDB API key:</strong> Free at themoviedb.org.<br><br><strong>API call logging:</strong> Logs every request/response to e2proxy — only for troubleshooting, combine with log level DEBUG.",
      "help.h_tech":"Technical Details & Raspberry Pi Notes","help.tech":"<strong>Recording lifecycle:</strong> EPG scan → regex match → Scheduled. Shortly before broadcast start: choose a proxy with a free tuner → <code>POST /api/record/start</code> → file path from response. Watchdog every 30s via <code>GET /api/record/status</code> → on completion: explicit stop to free the tuner.<br><br><strong>No own ffmpeg:</strong> All recordings run in e2proxy. The storage location is determined by e2proxy (<code>recordings_path</code>).<br><br><strong>Data:</strong> <code>~/e2recorder/data/</code> — config.json, series.json, recordings.json, tuner_history.json.<br><br><strong style=color:var(--red)>&#9888; Raspberry Pi — power supply & cable:</strong> Under load the Pi can freeze with a poor power supply (symptom: <code>hwmon: Undervoltage detected!</code> in dmesg).<br>&bull; <strong>Power supply:</strong> At least 27W USB-C (5V/5A) — e.g. Anker Nano II 65W<br>&bull; <strong>Cable:</strong> Short, thick USB-C cable — thin/long cables cause voltage drop under load<br>Check: <code>vcgencmd get_throttled</code> → should return <code>0x0</code>",
      "help.h_changelog":"Changelog"
    },
    de: {
      "nav.overview":"übersicht","nav.series":"serien","nav.movies":"filme","nav.recordings":"aufnahmen","nav.settings":"einstellungen","nav.help":"Hilfe",
      "hdr.proxy_connected":"{n} Proxy verbunden","hdr.no_proxy":"kein proxy — settings",
      "plan.title":"Aufnahmeplan","plan.scan":"EPG-Scan","plan.loading":"Laedt...",
      "plan.legend_rec":"Wird aufgenommen","plan.legend_live":"Laeuft gerade","plan.legend_other":"Sonstiges Programm",
      "plan.empty_noepg":"Keine EPG-Daten — EPG-Scan klicken","plan.empty_window":"Keine Sendungen im Zeitfenster","plan.no_epg":"Noch keine EPG-Daten",
      "tt.no_title":"(kein Titel)","tt.no_desc":"Keine Beschreibung verfügbar","tt.recording":"wird aufgenommen ({n})","tt.click_record":"Klicken zum Aufnehmen","tt.skip":"Diese Aufnahme auslassen",
      "qr.movie":"Film","qr.series":"Serie","qr.rec_movie":"Film aufnehmen","qr.movie_note":"Wird als Film in Plex Movies-Library importiert",
      "qr.this_episode":"Nur diese Folge","qr.once_note":"Einmalige Aufnahme dieser einen Folge",
      "qr.all_episodes":"ALLE FOLGEN — auch zukünftige","qr.pattern":"Suchmuster (Regex)","qr.full_title":"Vollst. Titel:","qr.shorten":"Kürzen für mehr Treffer: z.B.","qr.rec_all":"Alle Folgen aufnehmen",
      "toast.dup_planned":"Bereits vorhanden — Aufnahme geplant","toast.movie_planned":"Film-Aufnahme geplant","toast.once_planned":"Einmalige Aufnahme geplant","toast.series_created":"Serie eingerichtet",
      "toast.error":"Fehler: {n}","toast.saved":"Gespeichert","toast.proxy_added":"Proxy hinzugefuegt","toast.cleanup_started":"Cleanup gestartet",
      "toast.dups_removed":"{n} Duplikate entfernt","toast.entries_deleted":"{n} Eintraege geloescht","toast.rescan":"Rescan: {n}",
      "toast.url_copied":"URL kopiert","toast.no_stream":"Kein Stream verfuegbar","toast.no_url":"Keine URL verfuegbar","toast.no_stream_url":"Keine Stream-URL",
      "toast.required":"Name und Sender sind Pflichtfelder!","toast.dup_series":"Serie {n} auf diesem Sender bereits vorhanden!",
      "toast.api_log":"API-Logging: {n}","toast.api_log_hint":"Log-Level auf DEBUG setzen fuer volle Details","toast.log_level":"Log-Level: {n}",
      "confirm.delete_series":"Serie {n} entfernen? Bereits aufgenommene Dateien bleiben erhalten.","confirm.clear_failed":"Alle Fehler-Eintraege aus der Liste entfernen? Aufgenommene Dateien bleiben erhalten.","confirm.delete_rec":"Eintrag aus der Liste entfernen? Die aufgenommene Datei bleibt erhalten.","confirm.remove_proxy":"Proxy aus der Liste entfernen?",
      "status.scheduled":"Geplant","status.recording":"Laeuft","status.done":"Fertig","status.failed":"Fehler","status.skipped":"Ausgelassen","status.missed":"Verpasst","status.unknown":"?",
      "filter.all":"Alle",
      "sort.date_desc":"↓ Datum","sort.date_asc":"↑ Datum","sort.series_asc":"↑ Serie","sort.series_desc":"↓ Serie","sort.title_asc":"↑ Titel","sort.title_desc":"↓ Titel","sort.status":"Status",
      "th.name":"Name","th.type":"Typ","th.channel":"Sender","th.regex":"Regex","th.keep":"Keep","th.status":"Status","th.action":"Aktion","th.series":"Serie","th.title":"Titel","th.start":"Start","th.end":"Ende","th.mb":"MB","th.proxy":"Proxy",
      "series.title":"Serien","movies.title":"Filme","common.new":"+ Neu","table.empty":"Noch keine Einträge",
      "badge.movie":"Film","badge.series":"Serie","badge.once":"Einmalig","badge.active":"Aktiv","badge.inactive":"Inaktiv",
      "common.all":"alle","common.edit":"Bearbeiten","common.remove":"Entfernen","common.close":"Schliessen","common.name":"Name","common.add":"Hinzufuegen","common.save":"Speichern","common.search":"Suchen","common.yes":"Ja","common.no":"Nein","common.days":"Tage","common.on_caps":"AN","common.off_caps":"AUS",
      "rec.title":"Aufnahmen","rec.refresh":"Refresh","rec.refresh_title":"Aktualisieren","rec.clear_failed":"Fehler loeschen","rec.clear_failed_title":"Alle Fehler-Aufnahmen loeschen","rec.rescan":"Rescan","rec.rescan_title":"Status aller Aufnahmen vom Proxy abrufen","rec.scanning":"Scanne...",
      "rec.empty":"Keine Aufnahmen","rec.remaining":"noch ~{n} min","rec.info_title":"Informationen","rec.protect_title":"Schuetzen","rec.unprotect_title":"Schutz aufheben","rec.delete_title":"Eintrag loeschen (Datei bleibt erhalten)","rec.file_missing_title":"Datei nicht gefunden","rec.sort_title":"Sortierung",
      "player.fullscreen":"Vollbild","player.no_video":"Ihr Browser unterstuetzt kein Video.",
      "detail.status":"Status","detail.source":"Quelle","detail.start":"Start","detail.end":"Ende","detail.channel":"Sender","detail.proxy":"Proxy","detail.tuner":"Tuner","detail.shared":"shared","detail.filesize":"Dateigröße","detail.file":"Datei","detail.exists":"vorhanden","detail.missing":"fehlt","detail.filepath":"DATEIPFAD","detail.no_path":"kein Pfad",
      "proxy.empty_cfg":"Keine Proxies konfiguriert","proxy.busy":"belegt ({n})","proxy.free":"frei","proxy.free_total":"{a} frei / {b} gesamt","proxy.offline":"Nicht erreichbar","proxy.disable":"Deaktivieren","proxy.enable":"Aktivieren","proxy.off":"Aus","proxy.on":"Ein","proxy.free_short":"{a}/{b} frei","proxy.searching":"Suche...","proxy.ssdp_discovery":"SSDP-Discovery","proxy.offline_l":"offline",
      "tuner.recorded_via":"Aufgenommen via {n}",
      "settings.appearance":"Erscheinungsbild","settings.theme":"Theme","settings.theme_desc":"Hell oder Dunkel — wie im e2proxy","settings.dark":"Dunkel","settings.light":"Hell","settings.language":"Sprache","settings.language_desc":"Sprache der Oberfläche — wie im e2proxy",
      "settings.proxies":"Proxies","settings.proxy_url":"Proxy URL","settings.recordings":"Aufnahmen","settings.subdir":"Aufnahme-Unterordner (optional)","settings.subdir_ph":"leer = e2proxy Standard-Pfad","settings.subdir_help":"Leer = e2proxy schreibt in seinen eigenen Pfad. Optional: Unterordner relativ dazu, z.B. <code>Serien</code>",
      "settings.profile":"Stream-Profil","settings.profile_help":"Wird aus Proxy geladen","settings.pre_buffer":"Pre-Buffer (Sek)","settings.post_buffer":"Post-Buffer (Sek)",
      "settings.cleanup":"Aufraeum-Strategie","settings.trigger":"Trigger","settings.trig_onnew":"Bei neuer Aufnahme","settings.trig_daily":"Taeglich","settings.trig_never":"Nie","settings.hour":"Uhrzeit","settings.cleanup_now":"Keep-Last Cleanup","settings.dedup":"Duplikate bereinigen",
      "settings.epg_tmdb":"EPG & TMDB","settings.scan_interval":"Scan-Intervall (Sek)","settings.lookahead":"Vorschau (Stunden)","settings.tmdb_key":"TMDB API Key","settings.tmdb_lang":"TMDB Sprache",
      "settings.logs":"Logs","settings.log_files":"LOG-DATEIEN","settings.tuner_history":"Tuner-History","settings.log_level":"Log-Level","settings.log_level_desc":"DEBUG zeigt alle internen Abläufe — ideal für Troubleshooting. Nach der Diagnose wieder auf INFO setzen.","settings.log_retention":"Log-Dateien aufbewahren:","settings.log_path_help":"Logs liegen in <code>/data/logs/</code> — täglich rotiert","settings.api_log":"API-Call Logging","settings.api_log_desc":"Loggt jeden Request/Response zum e2proxy — nur für Troubleshooting einschalten",
      "logs.empty":"Keine Eintraege","logs.no_files":"Noch keine Dateien (kommen nach Mitternacht)","settings.current":"Aktuell: {n}",
      "modal.tmdb":"TMDB-Suche (optional)","modal.tmdb_ph":"Serienname suchen...","modal.name":"Serienname","modal.channel":"Sender","modal.choose_channel":"-- Sender waehlen --","modal.regex":"Regex-Pattern","modal.regex_help":"z.B. <code>GNTM|Germany.s Next Topmodel</code> · <code>^Zwischen Tuell</code>","modal.type":"Typ","modal.type_series":"Serie","modal.type_movie":"Film","modal.active":"Aktiv","modal.keep":"Keep Last (0=alle)","modal.year":"Jahr (für Filme)","modal.year_ph":"z.B. 2018","modal.pre":"Aufnahme früher starten (Sek.)","modal.pre_help":"Positiv = früher starten, negativ = später","modal.post":"Aufnahme länger laufen (Sek.)","modal.post_help":"Positiv = länger laufen, negativ = früher beenden","modal.add_series":"Serie hinzufuegen","modal.add_movie":"Film hinzufuegen","modal.edit_series":"Serie bearbeiten","modal.edit_movie":"Film bearbeiten","modal.saved":"(gespeichert)",
      "tmdb.no_results":"Keine Treffer","tmdb.de":"DE:","tmdb.regex":"Regex:",
      "help.title":"Hilfe & Changelog","help.back":"Zurück","help.subtitle":"Hilfe & Changelog",
      "help.h_overview":"Overview","help.overview":"Zeigt alle Sender als horizontale Zeitachse. Blaue Blöcke = geplante Aufnahmen, Grün = läuft.<br><br><strong>Klick auf Slot:</strong> Einmalig aufnehmen oder als Serie einplanen. Auch Slots ohne EPG-Beschreibung sind klickbar.<br><br><strong>Hover:</strong> Titel, Zeit, Beschreibung. Bei geplanten Aufnahmen: Auslassen-Button.<br><br><strong>EPG-Scan:</strong> Lädt frische Daten vom e2proxy, läuft automatisch stündlich.",
      "help.h_series":"Serien & Filme","help.series":"<strong>Typ:</strong> Bei jeder Aufnahme wählst du Film oder Serie. Beeinflusst Pfad/Library in Plex (Movies vs. TV Shows) und .nfo Format.<br><br><strong>Modus:</strong> Bei Serien zusätzlich nur diese Folge oder alle Folgen.<br><br><strong>Regex-Pattern</strong> bestimmt welche EPG-Titel aufgenommen werden (case-insensitive):<br><code>Die Geissens</code> alle Folgen &bull; <code>GNTM|Germany.s Next Topmodel</code> beide Schreibweisen &bull; <code>^Tatort</code> nur Titel die mit Tatort beginnen<br><br><strong>Pre/Post-Offset (Sek.):</strong> Pro Serie konfigurierbar. Manche Sender starten ihre Sendungen regelmäßig 30s früher oder enden später. Positive Werte = früher starten / länger laufen.<br><br><strong>Back-to-Back:</strong> Folgt eine Aufnahme direkt auf eine andere auf dem gleichen Sender (unter 60s Lücke), startet sie automatisch 2 Minuten früher — vermeidet Time-Drift.<br><br><strong>Keep Last:</strong> Behält nur die letzten N Aufnahmen, ältere werden automatisch gelöscht (inkl. .nfo). 0 = alle.<br><br><strong>TMDB-Suche:</strong> Liefert deutschen Titel, Aliases und Regex-Vorschlag.<br><br><strong>Duplikat-Schutz:</strong> Gleicher Name + Sender kann nicht doppelt angelegt werden.",
      "help.h_recordings":"Aufnahmen","help.recordings":"Sortierbar nach Datum, Serie, Titel, Status. Filterbar nach Status.<br><br><strong>Status:</strong> Geplant · <span style=color:var(--accent2)>Läuft</span> · <span style=color:var(--green)>Fertig</span> · <span style=color:var(--red)>Fehler</span> · <span style=color:var(--yellow)>Verpasst</span> · Ausgelassen<br><br><strong>Detail:</strong> Dateipfad, Größe, Proxy, Quelle (epg-scheduler / ui-quickrecord).<br><br><strong>Play:</strong> Aufnahme im Browser abspielen (benötigt <code>/recording/stream</code> im e2proxy).<br><br><strong>Rescan:</strong> Prüft Proxy-Status + Dateisystem, aktualisiert fehlende Dateien.<br><br><strong>Fehler löschen:</strong> Entfernt alle Einträge mit Status Fehler, Verpasst, Ausgelassen und fehlenden Dateien.",
      "help.h_settings":"Settings","help.settings":"<strong>Proxies:</strong> SSDP-Discovery oder manuelle URL. Mehrere Proxies möglich — der mit den meisten freien Tunern wird automatisch gewählt.<br><br><strong>Stream-Profil:</strong> Wird aus e2proxy geladen. Empfohlen: <code>remux-ac3</code>.<br><br><strong>Pre/Post-Buffer:</strong> Sekunden vor/nach EPG-Zeit für ungenaue Sendezeiten.<br><br><strong>Aufräum-Trigger:</strong> Bei neuer Aufnahme empfohlen.<br><br><strong>TMDB API Key:</strong> Kostenlos unter themoviedb.org.<br><br><strong>API-Call Logging:</strong> Loggt jeden Request/Response zum e2proxy — nur für Troubleshooting, kombinieren mit Log-Level DEBUG.",
      "help.h_tech":"Technische Details & Raspberry Pi Hinweise","help.tech":"<strong>Aufnahme-Lifecycle:</strong> EPG-Scan → Regex-Match → Geplant. Kurz vor Sendestart: Proxy mit freiem Tuner wählen → <code>POST /api/record/start</code> → Dateipfad aus Response. Watchdog alle 30s via <code>GET /api/record/status</code> → bei Fertig: expliziter Stop für Tuner-Freigabe.<br><br><strong>Kein eigenes ffmpeg:</strong> Alle Aufnahmen laufen im e2proxy. Speicherort wird vom e2proxy bestimmt (<code>recordings_path</code>).<br><br><strong>Daten:</strong> <code>~/e2recorder/data/</code> — config.json, series.json, recordings.json, tuner_history.json.<br><br><strong style=color:var(--red)>&#9888; Raspberry Pi — Netzteil & Kabel:</strong> Unter Last kann der Pi bei schlechter Stromversorgung einfrieren (Symptom: <code>hwmon: Undervoltage detected!</code> in dmesg).<br>&bull; <strong>Netzteil:</strong> Mindestens 27W USB-C (5V/5A) — z.B. Anker Nano II 65W<br>&bull; <strong>Kabel:</strong> Kurzes, dickes USB-C Kabel — dünne/lange Kabel verursachen Spannungsabfall unter Last<br>Prüfen: <code>vcgencmd get_throttled</code> → sollte <code>0x0</code> zurückgeben",
      "help.h_changelog":"Changelog"
    }
  };
  function detect(){
    try { var s = localStorage.getItem('e2recorder-lang'); if (s === 'en' || s === 'de') return s; } catch(e){}
    var n = (navigator.language || 'en').toLowerCase();
    if (n.indexOf('de') === 0) return 'de';
    return 'en';
  }
  var LANG = detect();
  window.getLang = function(){ return LANG; };
  window.LC = function(){ return LANG === 'de' ? 'de-DE' : 'en-US'; };
  window.t = function(key, vars){
    var s = (DICT[LANG] && DICT[LANG][key]);
    if (s === undefined) s = DICT.en[key];
    if (s === undefined) s = key;
    if (vars) { for (var k in vars) { s = s.split('{'+k+'}').join(vars[k]); } }
    return s;
  };
  window.setLang = function(l){
    try { localStorage.setItem('e2recorder-lang', l); } catch(e){}
    location.reload();
  };
  window.applyI18n = function(){
    document.querySelectorAll('[data-i18n]').forEach(function(el){
      try { el.textContent = t(el.getAttribute('data-i18n')); } catch(e){}
    });
    document.querySelectorAll('[data-i18n-html]').forEach(function(el){
      try { el.innerHTML = t(el.getAttribute('data-i18n-html')); } catch(e){}
    });
    document.querySelectorAll('[data-i18n-ph]').forEach(function(el){
      try { el.setAttribute('placeholder', t(el.getAttribute('data-i18n-ph'))); } catch(e){}
    });
    document.querySelectorAll('[data-i18n-title]').forEach(function(el){
      try { el.setAttribute('title', t(el.getAttribute('data-i18n-title'))); } catch(e){}
    });
    try {
      document.documentElement.setAttribute('lang', LANG);
      var sel = document.getElementById('lang-sel');
      if (sel) sel.value = LANG;
    } catch(e){}
  };
  document.addEventListener('DOMContentLoaded', function(){ try { applyI18n(); } catch(e){} });
})();
</script>"""


# ── Web UI HTML ────────────────────────────────────────────────────────────

_HTML = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>e2recorder</title>
<style>{CSS}</style>
{I18N}
</head>
<body>
<script>(function(){var t=localStorage.getItem('e2recorder-theme');if(t==='light')document.documentElement.setAttribute('data-theme','light');})();</script>
<header>
  <div class="logo">e2<span>recorder</span></div>
  <div class="sub" id="proxy-status-header" style="margin-left:12px">...</div>
  <nav>
    <button class="active" onclick="switchTab('plan',this)" data-i18n="nav.overview">overview</button>
    <button onclick="switchTab('series',this)" data-i18n="nav.series">serien</button>
    <button onclick="switchTab('movies',this)" data-i18n="nav.movies">filme</button>
    <button onclick="switchTab('recordings',this)" data-i18n="nav.recordings">aufnahmen</button>
    <button onclick="switchTab('settings',this)" data-i18n="nav.settings">settings</button>
    <button onclick="switchTab('help',this)" data-i18n-title="nav.help" title="Hilfe" style="font-size:1rem;padding:7px 14px">?</button>
  </nav>
</header>
<div class="toast" id="toast"></div>
<div class="container">
  <div class="tab active" id="tab-plan">
    <div class="card">
      <div class="card-header">
        <span class="card-title" data-i18n="plan.title">Aufnahmeplan</span>
        <div style="display:flex;gap:8px;align-items:center">
          <select id="plan-hours" onchange="loadPlan()" style="width:auto;padding:6px 10px">
            <option value="6">6h</option><option value="12">12h</option>
            <option value="24" selected>24h</option><option value="48">48h</option>
          </select>
          <button class="btn btn-primary" onclick="triggerScan(this)">&#8635; <span data-i18n="plan.scan">EPG-Scan</span></button>
        </div>
      </div>
      <div class="plan-day-tabs" id="plan-day-tabs"></div>
      <div class="plan-outer">
        <div class="plan-scroll-wrap" id="plan-scroll-wrap">
          <div class="plan-time-header" id="plan-time-header"></div>
          <div class="plan-grid" id="plan-grid"></div>
        </div>
      </div>
      <div class="epg-tooltip" id="epg-tooltip"></div>
      <div class="plan-legend">
        <span><span class="legend-box" style="background:rgba(88,166,255,.18);border:1px solid rgba(88,166,255,.4)"></span><span data-i18n="plan.legend_rec">Wird aufgenommen</span></span>
        <span><span class="legend-box" style="background:rgba(46,160,67,.15);border:1px solid rgba(46,160,67,.4)"></span><span data-i18n="plan.legend_live">Laeuft gerade</span></span>
        <span><span class="legend-box" style="background:rgba(139,148,158,.08);border:1px solid rgba(139,148,158,.15)"></span><span data-i18n="plan.legend_other">Sonstiges Programm</span></span>
      </div>
    </div>
  </div>

  <div class="tab" id="tab-series">
    <div class="card">
      <div class="card-header">
        <span class="card-title" data-i18n="series.title">Serien</span>
        <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
          <button class="btn btn-primary" onclick="openAddModal('series')" data-i18n="common.new">+ Neu</button>
        </div>
      </div>
      <table>
        <thead><tr><th data-i18n="th.name">Name</th><th data-i18n="th.type">Typ</th><th data-i18n="th.channel">Sender</th><th data-i18n="th.regex">Regex</th><th data-i18n="th.keep">Keep</th><th data-i18n="th.status">Status</th><th data-i18n="th.action">Aktion</th></tr></thead>
        <tbody id="series-tbody"></tbody>
      </table>
    </div>
  </div>

  <div class="tab" id="tab-movies">
    <div class="card">
      <div class="card-header">
        <span class="card-title" data-i18n="movies.title">Filme</span>
        <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
          <button class="btn btn-primary" onclick="openAddModal('movie')" data-i18n="common.new">+ Neu</button>
        </div>
      </div>
      <table>
        <thead><tr><th data-i18n="th.name">Name</th><th data-i18n="th.type">Typ</th><th data-i18n="th.channel">Sender</th><th data-i18n="th.regex">Regex</th><th data-i18n="th.keep">Keep</th><th data-i18n="th.status">Status</th><th data-i18n="th.action">Aktion</th></tr></thead>
        <tbody id="movies-tbody"></tbody>
      </table>
    </div>
  </div>

  <div class="tab" id="tab-recordings">
    <div class="card">
      <div class="card-header">
        <span class="card-title" data-i18n="rec.title">Aufnahmen</span>
        <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
          <button class="btn btn-sm" onclick="loadRecordings()" data-i18n-title="rec.refresh_title" title="Aktualisieren">&#8635; <span data-i18n="rec.refresh">Refresh</span></button>
          <button class="btn btn-sm btn-danger" onclick="clearFailedRecs()" data-i18n-title="rec.clear_failed_title" title="Alle Fehler-Aufnahmen loeschen">&#128465; <span data-i18n="rec.clear_failed">Fehler loeschen</span></button>
          <button class="btn btn-sm" onclick="rescueScan()" data-i18n-title="rec.rescan_title" title="Status aller Aufnahmen vom Proxy abrufen">&#128269; <span data-i18n="rec.rescan">Rescan</span></button>
          <select id="rec-sort" onchange="loadRecordings()" style="width:auto;padding:5px 8px" data-i18n-title="rec.sort_title" title="Sortierung">
            <option value="start_desc" data-i18n="sort.date_desc">&#8595; Datum</option>
            <option value="start_asc" data-i18n="sort.date_asc">&#8593; Datum</option>
            <option value="serie_asc" data-i18n="sort.series_asc">&#8593; Serie</option>
            <option value="serie_desc" data-i18n="sort.series_desc">&#8595; Serie</option>
            <option value="title_asc" data-i18n="sort.title_asc">&#8593; Titel</option>
            <option value="title_desc" data-i18n="sort.title_desc">&#8595; Titel</option>
            <option value="status" data-i18n="sort.status">Status</option>
          </select>
          <select id="rec-filter" onchange="loadRecordings()" style="width:auto;padding:5px 8px">
          <option value="" data-i18n="filter.all">Alle</option>
          <option value="scheduled" data-i18n="status.scheduled">Geplant</option>
          <option value="recording" data-i18n="status.recording">Laeuft</option>
          <option value="done" data-i18n="status.done">Fertig</option>
          <option value="failed" data-i18n="status.failed">Fehler</option>
          <option value="skipped" data-i18n="status.skipped">Ausgelassen</option>
          <option value="missed" data-i18n="status.missed">Verpasst</option>
          </select>
        </div>
      </div>
      <table>
        <thead><tr><th data-i18n="th.series">Serie</th><th data-i18n="th.title">Titel</th><th data-i18n="th.start">Start</th><th data-i18n="th.end">Ende</th><th data-i18n="th.mb">MB</th><th data-i18n="th.proxy">Proxy</th><th data-i18n="th.status">Status</th><th data-i18n="th.action">Aktion</th></tr></thead>
        <tbody id="rec-tbody"></tbody>
      </table>
    </div>
  </div>

  <div class="tab" id="tab-settings">
    <div class="card">
      <div class="card-title" style="margin-bottom:12px" data-i18n="settings.appearance">Erscheinungsbild</div>
      <div style="display:flex;align-items:center;justify-content:space-between;padding:8px 0">
        <div>
          <div style="font-size:.85rem;font-weight:500" data-i18n="settings.theme">Theme</div>
          <div class="help-text" data-i18n="settings.theme_desc">Hell oder Dunkel &mdash; wie im e2proxy</div>
        </div>
        <div style="display:flex;gap:6px">
          <button class="btn btn-sm" id="theme-dark-btn" onclick="setTheme('dark')">&#127769; <span data-i18n="settings.dark">Dunkel</span></button>
          <button class="btn btn-sm" id="theme-light-btn" onclick="setTheme('light')">&#9728; <span data-i18n="settings.light">Hell</span></button>
        </div>
      </div>
      <div style="display:flex;align-items:center;justify-content:space-between;padding:8px 0;border-top:1px solid var(--border)">
        <div>
          <div style="font-size:.85rem;font-weight:500" data-i18n="settings.language">Sprache</div>
          <div class="help-text" data-i18n="settings.language_desc">Sprache der Oberfl&auml;che &mdash; wie im e2proxy</div>
        </div>
        <select id="lang-sel" onchange="setLang(this.value)" style="width:auto;padding:4px 8px;font-size:.8rem"><option value="en">🇬🇧 EN</option><option value="de">🇩🇪 DE</option></select>
      </div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:12px" data-i18n="settings.proxies">Proxies</div>
      <div id="settings-proxies-list" style="margin-bottom:10px"></div>
      <div class="form-row">
        <div><label data-i18n="settings.proxy_url">Proxy URL</label><input id="settings-proxy-url" type="text" placeholder="http://192.168.88.67:8888"></div>
        <div><label data-i18n="common.name">Name</label><input id="settings-proxy-name" type="text" placeholder="Wien"></div>
      </div>
      <div style="display:flex;gap:8px">
        <button class="btn btn-primary" onclick="addProxyFromSettings()" data-i18n="common.add">Hinzufuegen</button>
        <button class="btn" onclick="runDiscovery()">&#8634; SSDP</button>
      </div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:16px" data-i18n="settings.recordings">Aufnahmen</div>
      <div class="form-row">
        <div>
          <label data-i18n="settings.subdir">Aufnahme-Unterordner (optional)</label>
          <input id="cfg-recdir" type="text" data-i18n-ph="settings.subdir_ph" placeholder="leer = e2proxy Standard-Pfad">
          <div class="help-text" data-i18n-html="settings.subdir_help">Leer = e2proxy schreibt in seinen eigenen Pfad. Optional: Unterordner relativ dazu, z.B. <code>Serien</code></div>
        </div>
        <div>
          <label data-i18n="settings.profile">Stream-Profil</label>
          <select id="cfg-profile">
            <option value="remux-ac3">remux-ac3</option>
          </select>
          <div class="help-text" data-i18n="settings.profile_help">Wird aus Proxy geladen</div>
        </div>
      </div>
      <div class="form-row">
        <div><label data-i18n="settings.pre_buffer">Pre-Buffer (Sek)</label><input id="cfg-pre" type="number" min="0" max="300"></div>
        <div><label data-i18n="settings.post_buffer">Post-Buffer (Sek)</label><input id="cfg-post" type="number" min="0" max="600"></div>
      </div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:16px" data-i18n="settings.cleanup">Aufraeum-Strategie</div>
      <div class="form-row">
        <div><label data-i18n="settings.trigger">Trigger</label>
          <select id="cfg-cleanup" onchange="updateCleanupUi()">
            <option value="on_new" data-i18n="settings.trig_onnew">Bei neuer Aufnahme</option>
            <option value="daily" data-i18n="settings.trig_daily">Taeglich</option>
            <option value="never" data-i18n="settings.trig_never">Nie</option>
          </select>
        </div>
        <div id="cleanup-hour-wrap"><label data-i18n="settings.hour">Uhrzeit</label><input id="cfg-cleanup-hour" type="number" min="0" max="23"></div>
      </div>
      <div style="display:flex;gap:8px;margin-top:8px"><button class="btn btn-sm" onclick="triggerCleanupNow()" data-i18n="settings.cleanup_now">Keep-Last Cleanup</button><button class="btn btn-sm btn-danger" onclick="deduplicateDb()" data-i18n="settings.dedup">Duplikate bereinigen</button></div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:16px" data-i18n="settings.epg_tmdb">EPG &amp; TMDB</div>
      <div class="form-row">
        <div><label data-i18n="settings.scan_interval">Scan-Intervall (Sek)</label><input id="cfg-epg-interval" type="number" min="300"></div>
        <div><label data-i18n="settings.lookahead">Vorschau (Stunden)</label><input id="cfg-epg-lookahead" type="number" min="2" max="168"></div>
      </div>
      <div class="form-row">
        <div><label data-i18n="settings.tmdb_key">TMDB API Key</label><input id="cfg-tmdb-key" type="password"></div>
        <div><label data-i18n="settings.tmdb_lang">TMDB Sprache</label>
          <select id="cfg-tmdb-lang">
            <option value="de-DE">Deutsch</option>
            <option value="en-US">English</option>
          </select>
        </div>
      </div>
    </div>
    <div style="display:flex;justify-content:flex-end">
      <button class="btn btn-primary" onclick="saveAllConfig()" data-i18n="common.save">Speichern</button>
    </div>

    <!-- Logs in Settings -->
    <div class="card">
      <div class="card-header">
        <span class="card-title" data-i18n="settings.logs">Logs</span>
        <div style="display:flex;gap:6px;align-items:center">
          <select id="log-level" onchange="loadLogs()" style="width:auto;padding:4px 8px;font-size:.75rem">
            <option value="DEBUG">DEBUG</option>
            <option value="INFO" selected>INFO</option>
            <option value="WARNING">WARNING</option>
            <option value="ERROR">ERROR</option>
          </select>
          <button class="btn btn-sm" onclick="loadLogs()">&#8635;</button>
        </div>
      </div>
      <div id="log-container" style="max-height:300px;overflow-y:auto;background:var(--bg);border-radius:5px;padding:10px;font-family:'JetBrains Mono',monospace;font-size:.72rem"></div>
      <div style="margin-top:10px">
        <div style="font-family:'JetBrains Mono',monospace;font-size:.7rem;color:var(--muted);margin-bottom:6px" data-i18n="settings.log_files">LOG-DATEIEN</div>
        <div id="log-files-list" style="font-size:.75rem;color:var(--muted);font-family:'JetBrains Mono',monospace"></div>
      </div>
    </div>

    <!-- Tuner History -->
    <div class="card">
      <div class="card-header">
        <span class="card-title" data-i18n="settings.tuner_history">Tuner-History</span>
        <button class="btn btn-sm" onclick="loadTunerHistory()">&#8635;</button>
      </div>
      <div id="tuner-history-container" style="max-height:280px;overflow-y:auto"></div>
    </div>

    <!-- Log-Level Steuerung -->
    <div class="card">
      <div class="card-title" style="margin-bottom:12px" data-i18n="settings.log_level">Log-Level</div>
      <p style="font-size:.78rem;color:var(--muted);margin-bottom:10px" data-i18n="settings.log_level_desc">
        DEBUG zeigt alle internen Abläufe — ideal für Troubleshooting.
        Nach der Diagnose wieder auf INFO setzen.
      </p>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px">
        <button class="btn btn-sm" onclick="setLogLevel('DEBUG')" style="border-color:var(--muted);color:var(--muted)">DEBUG</button>
        <button class="btn btn-sm" onclick="setLogLevel('INFO')" style="border-color:var(--blue);color:var(--blue)">INFO</button>
        <button class="btn btn-sm" onclick="setLogLevel('WARNING')" style="border-color:var(--yellow);color:var(--yellow)">WARNING</button>
        <button class="btn btn-sm" onclick="setLogLevel('ERROR')" style="border-color:var(--red);color:var(--red)">ERROR</button>
      </div>
      <div id="log-level-status" style="font-size:.72rem;font-family:'JetBrains Mono',monospace;color:var(--muted);margin-bottom:10px"></div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
        <label style="margin:0;white-space:nowrap" data-i18n="settings.log_retention">Log-Dateien aufbewahren:</label>
        <input id="cfg-log-retention" type="number" min="1" max="365"
          style="width:80px;padding:4px 8px;font-size:.82rem">
        <span style="font-size:.78rem;color:var(--muted)" data-i18n="common.days">Tage</span>
      </div>
      <div class="help-text" data-i18n-html="settings.log_path_help">Logs liegen in <code>/data/logs/</code> — täglich rotiert</div>
      <div style="display:flex;align-items:center;justify-content:space-between;padding:8px 0;border-top:1px solid var(--border)">
        <div>
          <div style="font-size:.82rem;font-weight:500" data-i18n="settings.api_log">API-Call Logging</div>
          <div class="help-text" data-i18n="settings.api_log_desc">Loggt jeden Request/Response zum e2proxy — nur für Troubleshooting einschalten</div>
        </div>
        <button id="api-log-btn" class="btn btn-sm" onclick="toggleApiLogging()" data-i18n="proxy.on">Ein</button>
      </div>
    </div>
  </div>

  <div class="tab" id="tab-help">
    <div class="version-badge">v{VERSION}</div>
    <div class="help-grid">

      <div class="card">
        <div class="card-title" data-i18n="help.h_overview">Overview</div>
        <p data-i18n-html="help.overview">Zeigt alle Sender als horizontale Zeitachse. Blaue Blöcke = geplante Aufnahmen, Grün = läuft.<br><br>
        <strong>Klick auf Slot:</strong> Einmalig aufnehmen oder als Serie einplanen. Auch Slots ohne EPG-Beschreibung sind klickbar.<br><br>
        <strong>Hover:</strong> Titel, Zeit, Beschreibung. Bei geplanten Aufnahmen: "Auslassen"-Button.<br><br>
        <strong>EPG-Scan:</strong> Lädt frische Daten vom e2proxy, läuft automatisch stündlich.</p>
      </div>

      <div class="card">
        <div class="card-title" data-i18n="help.h_series">Serien &amp; Filme</div>
        <p data-i18n-html="help.series"><strong>Typ:</strong> Bei jeder Aufnahme wählst du Film &#127909; oder Serie &#128250;.
        Beeinflusst Pfad/Library in Plex (Movies vs. TV Shows) und .nfo Format.<br><br>
        <strong>Modus:</strong> Bei Serien zusätzlich "nur diese Folge" oder "alle Folgen".<br><br>
        <strong>Regex-Pattern</strong> bestimmt welche EPG-Titel aufgenommen werden (case-insensitive):<br>
        <code>Die Geissens</code> alle Folgen &bull;
        <code>GNTM|Germany.s Next Topmodel</code> beide Schreibweisen &bull;
        <code>^Tatort</code> nur Titel die mit Tatort beginnen<br><br>
        <strong>Pre/Post-Offset (Sek.):</strong> Pro Serie konfigurierbar. Manche Sender starten ihre Sendungen
        regelmäßig 30s früher oder enden später. Positive Werte = früher starten / länger laufen.<br><br>
        <strong>Back-to-Back:</strong> Folgt eine Aufnahme direkt auf eine andere auf dem gleichen Sender
        (≤60s Lücke), startet sie automatisch 2 Minuten früher — vermeidet Time-Drift.<br><br>
        <strong>Keep Last:</strong> Behält nur die letzten N Aufnahmen, ältere werden automatisch gelöscht (inkl. .nfo). 0 = alle.<br><br>
        <strong>TMDB-Suche:</strong> Liefert deutschen Titel, Aliases und Regex-Vorschlag.<br><br>
        <strong>Duplikat-Schutz:</strong> Gleicher Name + Sender kann nicht doppelt angelegt werden.</p>
      </div>

      <div class="card">
        <div class="card-title" data-i18n="help.h_recordings">Aufnahmen</div>
        <p data-i18n-html="help.recordings">Sortierbar nach Datum, Serie, Titel, Status. Filterbar nach Status.<br><br>
        <strong>Status:</strong> Geplant · <span style="color:var(--accent2)">Läuft</span> · <span style="color:var(--green)">Fertig</span> · <span style="color:var(--red)">Fehler</span> · <span style="color:var(--yellow)">Verpasst</span> · Ausgelassen<br><br>
        <strong>&#128269; Detail:</strong> Dateipfad, Größe, Proxy, Quelle (epg-scheduler / ui-quickrecord).<br><br>
        <strong>&#9654; Play:</strong> Aufnahme im Browser abspielen (benötigt <code>/recording/stream</code> im e2proxy).<br><br>
        <strong>Rescan:</strong> Prüft Proxy-Status + Dateisystem, aktualisiert fehlende Dateien.<br><br>
        <strong>Fehler löschen:</strong> Entfernt alle Einträge mit Status Fehler, Verpasst, Ausgelassen und fehlenden Dateien.</p>
      </div>

      <div class="card">
        <div class="card-title" data-i18n="help.h_settings">Settings</div>
        <p data-i18n-html="help.settings"><strong>Proxies:</strong> SSDP-Discovery oder manuelle URL. Mehrere Proxies möglich — der mit den meisten freien Tunern wird automatisch gewählt.<br><br>
        <strong>Stream-Profil:</strong> Wird aus e2proxy geladen. Empfohlen: <code>remux-ac3</code>.<br><br>
        <strong>Pre/Post-Buffer:</strong> Sekunden vor/nach EPG-Zeit für ungenaue Sendezeiten.<br><br>
        <strong>Aufräum-Trigger:</strong> "Bei neuer Aufnahme" empfohlen.<br><br>
        <strong>TMDB API Key:</strong> Kostenlos unter themoviedb.org.<br><br>
        <strong>API-Call Logging:</strong> Loggt jeden Request/Response zum e2proxy — nur für Troubleshooting, kombinieren mit Log-Level DEBUG.</p>
      </div>

      <div class="card help-grid-full">
        <div class="card-title" data-i18n="help.h_tech">Technische Details &amp; Raspberry Pi Hinweise</div>
        <p data-i18n-html="help.tech"><strong>Aufnahme-Lifecycle:</strong> EPG-Scan → Regex-Match → "Geplant". Kurz vor Sendestart: Proxy mit freiem Tuner wählen → <code>POST /api/record/start</code> → Dateipfad aus Response. Watchdog alle 30s via <code>GET /api/record/status</code> → bei Fertig: expliziter Stop für Tuner-Freigabe.<br><br>
        <strong>Kein eigenes ffmpeg:</strong> Alle Aufnahmen laufen im e2proxy. Speicherort wird vom e2proxy bestimmt (<code>recordings_path</code>).<br><br>
        <strong>Daten:</strong> <code>~/e2recorder/data/</code> — config.json, series.json, recordings.json, tuner_history.json.<br><br>
        <strong style="color:var(--red)">&#9888; Raspberry Pi — Netzteil &amp; Kabel:</strong>
        Unter Last kann der Pi bei schlechter Stromversorgung einfrieren (Symptom: <code>hwmon: Undervoltage detected!</code> in dmesg).<br>
        &bull; <strong>Netzteil:</strong> Mindestens 27W USB-C (5V/5A) — z.B. Anker Nano II 65W &#10003;<br>
        &bull; <strong>Kabel:</strong> Kurzes, dickes USB-C Kabel — dünne/lange Kabel verursachen Spannungsabfall unter Last<br>
        Prüfen: <code>vcgencmd get_throttled</code> → sollte <code>0x0</code> zurückgeben</p>
      </div>

      <div class="card help-grid-full">
        <div class="card-title" data-i18n="help.h_changelog">Changelog</div>

        <div class="cl-entry cl-current">
          <b style="font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--accent2)">v1.4.1</b>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-16</span>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">Hilfe als Tab · Sprache in Einstellungen</span>
          <div style="font-size:11px;margin-top:4px;color:var(--muted)">
            Hilfe &amp; Changelog jetzt als integrierter Tab in der gleichen Seite (kein separates Fenster mehr) — gleiches Design.
            Sprachauswahl in die Einstellungen verschoben (wie im e2proxy), direkt unter der Theme-Auswahl.
          </div>
        </div>

        <div class="cl-entry cl-old">
          <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.4.0</b>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-16</span>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">Mehrsprachige UI (EN/DE)</span>
          <div style="font-size:11px;margin-top:4px;color:var(--muted)">
            Komplette WebUI jetzt zweisprachig (Englisch/Deutsch) mit clientseitiger i18n ohne Server-Roundtrip.
            Sprachauswahl wird in localStorage gespeichert. Hilfe-Seite ebenfalls übersetzt.
          </div>
        </div>

        <div class="cl-entry cl-old">
          <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.3.2</b>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-16</span>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">Fix: "Auslassen"-Button erscheint wieder</span>
          <div style="font-size:11px;margin-top:4px;color:var(--muted)">
            Behoben: Bei Serien mit Doppelpunkt-Sender-Ref (z.B. First Dates) fehlte der "Diese Aufnahme auslassen"-Button im EPG-Tooltip.
            Ursache war ein Format-Mismatch (':' vs. '_') im internen Event-Key — jetzt normalisiert.
          </div>
        </div>

        <div class="cl-entry cl-old">
          <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.3.1</b>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-16</span>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">Tooltip-Fix · Auto-Close · Button-Text</span>
          <div style="font-size:11px;margin-top:4px;color:var(--muted)">
            EPG-Tooltip wird jetzt korrekt im Viewport positioniert — der "Auslassen"-Button bleibt auch bei langer Beschreibung sichtbar,
            Quick-Record Dialog schließt sich automatisch nach dem Einplanen,
            Button-Text generisch "Diese Aufnahme auslassen" (auch für Filme)
          </div>
        </div>

        <div class="cl-entry cl-old">
          <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.3.0</b>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-16</span>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">Getrennte Tabs Serien/Filme · Cache-Control</span>
          <div style="font-size:11px;margin-top:4px;color:var(--muted)">
            Eigene Tabs für Serien und Filme (statt kombiniert mit Filter),
            "+ Neu" öffnet jeweils den passenden Typ vorausgewählt,
            Cache-Control Header für die UI (kein veraltetes UI mehr aus dem Browser-Cache)
          </div>
        </div>

        <div class="cl-entry cl-old">
          <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.2.0</b>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-15</span>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">Filme/Serien · Pre/Post-Offsets · Back-to-Back · Channel-Fix</span>
          <div style="font-size:11px;margin-top:4px;color:var(--muted)">
            Movie/Series Typ-Auswahl im Quick-Record Modal,
            Pre-/Post-Offset Sekunden pro Serie konfigurierbar (für Sender die früher/später anfangen),
            Back-to-Back Erkennung auf gleichem Sender → 2 Min früher starten,
            Filter "Nur Serien"/"Nur Filme" im Serien-Tab,
            Channel-Selector Fix (Refs mit _ und : werden gematcht),
            year-Feld für Filme (für Plex Movie-Titel),
            erweiterte e2proxy v3.2 API mit kind/season/episode
          </div>
        </div>

        <div class="cl-entry cl-old">
          <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.1.0</b>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-10</span>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">/api/health · Receiver Tracking · File Logging · Intelligentes Tuner-Wait</span>
          <div style="font-size:11px;margin-top:4px;color:var(--muted)">
            /api/health für schnellere Proxy-Erkennung, Receiver-Tracking,
            Intelligentes Warten via remaining_sec, File-Logging mit täglicher Rotation,
            API-Call Logging Toggle, einmalige Aufnahmen (once=True)
          </div>
        </div>

        <div class="cl-entry cl-old">
          <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.0.0</b>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-09</span>
          <span style="color:var(--muted);font-size:10px;margin-left:8px">Erster stabiler Release · Docker · Recording · EPG · UI</span>
          <div style="font-size:11px;margin-top:4px;color:var(--muted)">
            Serien-Aufnahme-Scheduler via e2proxy API, EPG-Grid, TMDB, SSDP-Discovery,
            Multi-Proxy, Keep-Last-Cleanup, Docker (python:3.11-slim), Tuner-History,
            Duplikat-Schutz für Serien, Netzteil/Kabel Warnung
          </div>
        </div>

      </div>

    </div>
  </div>

</div>

<div class="modal-overlay" id="modal">
<div class="modal">
  <h2 id="modal-title">Serie</h2>
  <input type="hidden" id="edit-id">
  <div class="form-row one" style="margin-bottom:8px">
    <div>
      <label data-i18n="modal.tmdb">TMDB-Suche (optional)</label>
      <div style="display:flex;gap:8px">
        <input id="tmdb-query" type="text" data-i18n-ph="modal.tmdb_ph" placeholder="Serienname suchen...">
        <button class="btn btn-sm" onclick="doTmdbSearch()" style="white-space:nowrap" data-i18n="common.search">Suchen</button>
      </div>
    </div>
  </div>
  <div id="tmdb-results" class="tmdb-results" style="display:none"></div>
  <hr style="margin:16px 0">
  <div class="form-row one"><div><label data-i18n="modal.name">Serienname</label><input id="s-name" type="text"></div></div>
  <div style="margin-bottom:12px">
    <label data-i18n="modal.channel">Sender</label>
    <select id="s-ch-select" onchange="onChannelSelect(this)" style="width:100%">
      <option value="" data-i18n="modal.choose_channel">-- Sender waehlen --</option>
    </select>
    <input type="hidden" id="s-ch-ref"><input type="hidden" id="s-ch-name">
  </div>
  <div class="form-row one">
    <div>
      <label data-i18n="modal.regex">Regex-Pattern</label>
      <input id="s-regex" type="text" oninput="updateRegexPreview()">
      <div class="help-text" data-i18n-html="modal.regex_help">z.B. <code>GNTM|Germany.s Next Topmodel</code> &#183; <code>^Zwischen Tuell</code></div>
      <div id="regex-preview" class="regex-preview" style="display:none"></div>
    </div>
  </div>
  <div class="form-row">
    <div><label data-i18n="modal.type">Typ</label>
      <select id="s-kind">
        <option value="series" data-i18n="modal.type_series">Serie</option>
        <option value="movie" data-i18n="modal.type_movie">Film</option>
      </select>
    </div>
    <div><label data-i18n="modal.active">Aktiv</label><select id="s-enabled"><option value="true" data-i18n="common.yes">Ja</option><option value="false" data-i18n="common.no">Nein</option></select></div>
  </div>
  <div class="form-row">
    <div><label data-i18n="modal.keep">Keep Last (0=alle)</label><input id="s-keep" type="number" min="0" value="0"></div>
    <div><label data-i18n="modal.year">Jahr (für Filme)</label><input id="s-year" type="number" min="1900" max="2099" data-i18n-ph="modal.year_ph" placeholder="z.B. 2018"></div>
  </div>
  <div class="form-row">
    <div>
      <label data-i18n="modal.pre">Aufnahme früher starten (Sek.)</label>
      <input id="s-pre" type="number" min="-600" max="600" value="0">
      <div class="help-text" data-i18n="modal.pre_help">Positiv = früher starten, negativ = später</div>
    </div>
    <div>
      <label data-i18n="modal.post">Aufnahme länger laufen (Sek.)</label>
      <input id="s-post" type="number" min="-600" max="600" value="0">
      <div class="help-text" data-i18n="modal.post_help">Positiv = länger laufen, negativ = früher beenden</div>
    </div>
  </div>
  <div class="actions">
    <button class="btn" onclick="closeModal()" data-i18n="common.close">Schliessen</button>
    <button class="btn btn-primary" onclick="saveSerie()" data-i18n="common.save">Speichern</button>
  </div>
</div>
</div>
<script>{JS}</script>
</body>
</html>"""

def render_help():
    """Rendert die Hilfe-Seite als eigenständige HTML-Seite."""
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>e2recorder — Hilfe & Changelog</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>
:root{{--bg:#0a0a0f;--surface:#12121a;--surface2:#1a1a26;--border:#2a2a3d;--accent:#6366f1;--accent2:#818cf8;--text:#e2e2f0;--muted:#6b6b8a;--green:#22c55e;--red:#ef4444;--yellow:#f59e0b;}}
[data-theme="light"]{{--bg:#f4f5f7;--surface:#fff;--surface2:#f0f1f5;--border:#d1d5db;--accent:#4f46e5;--accent2:#4f46e5;--text:#1a1a2e;--muted:#6b7280;}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',system-ui,sans-serif;font-weight:300;background:var(--bg);color:var(--text);min-height:100vh}}
header{{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;gap:16px;position:sticky;top:0;z-index:10}}
.logo{{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:16px;color:var(--accent2)}}
.logo span{{color:var(--muted);font-weight:400}}
.back-btn{{margin-left:auto;background:none;border:1px solid var(--border);color:var(--muted);padding:5px 12px;border-radius:5px;cursor:pointer;font-family:'JetBrains Mono',monospace;font-size:.75rem;text-decoration:none;transition:all .15s}}
.back-btn:hover{{border-color:var(--accent);color:var(--accent2)}}
.container{{max-width:1200px;margin:0 auto;padding:28px 24px}}
.version-badge{{display:inline-block;font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700;color:var(--accent2);background:rgba(99,102,241,.12);border:1px solid rgba(99,102,241,.3);padding:3px 10px;border-radius:4px;margin-bottom:20px}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}}
.grid-full{{grid-column:1/-1}}
@media(max-width:700px){{.grid{{grid-template-columns:1fr}}}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px}}
.card-title{{font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:700;color:var(--accent2);text-transform:uppercase;letter-spacing:1px;margin-bottom:12px}}
p{{font-size:.82rem;line-height:1.7;color:var(--muted)}}
p strong{{color:var(--text)}}
code{{font-family:'JetBrains Mono',monospace;font-size:.78rem;color:var(--accent2);background:rgba(99,102,241,.08);padding:1px 5px;border-radius:3px}}
.cl-entry{{padding:10px 0;border-bottom:1px solid var(--border)}}
.cl-entry:last-child{{border-bottom:none}}
.cl-current{{border-left:3px solid var(--accent);padding-left:14px}}
.cl-old{{border-left:3px solid var(--border);padding-left:14px}}
</style>
<script>(function(){{var t=localStorage.getItem('e2recorder-theme');if(t==='light')document.documentElement.setAttribute('data-theme','light');}})();</script>
{I18N_JS}
</head>
<body>
<header>
  <div class="logo">e2<span>recorder</span></div>
  <span style="font-family:'JetBrains Mono',monospace;font-size:.75rem;color:var(--muted);margin-left:8px" data-i18n="help.subtitle">Hilfe &amp; Changelog</span>
  <select id="lang-sel" onchange="setLang(this.value)" style="margin-left:auto;width:auto;padding:4px 8px;font-size:.75rem;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:5px"><option value="en">🇬🇧 EN</option><option value="de">🇩🇪 DE</option></select>
  <a href="/" class="back-btn" style="margin-left:10px" data-i18n="help.back">&#8592; Zurück</a>
</header>
<div class="container">
  <div class="version-badge">v{VERSION}</div>

  <div class="grid">

    <div class="card">
      <div class="card-title" data-i18n="help.h_overview">Overview</div>
      <p data-i18n-html="help.overview">Zeigt alle Sender als horizontale Zeitachse. Blaue Blöcke = geplante Aufnahmen, Grün = läuft.<br><br>
      <strong>Klick auf Slot:</strong> Einmalig aufnehmen oder als Serie einplanen. Auch Slots ohne EPG-Beschreibung sind klickbar.<br><br>
      <strong>Hover:</strong> Titel, Zeit, Beschreibung. Bei geplanten Aufnahmen: "Auslassen"-Button.<br><br>
      <strong>EPG-Scan:</strong> Lädt frische Daten vom e2proxy, läuft automatisch stündlich.</p>
    </div>

    <div class="card">
      <div class="card-title" data-i18n="help.h_series">Serien &amp; Filme</div>
      <p data-i18n-html="help.series"><strong>Typ:</strong> Bei jeder Aufnahme wählst du Film &#127909; oder Serie &#128250;.
      Beeinflusst Pfad/Library in Plex (Movies vs. TV Shows) und .nfo Format.<br><br>
      <strong>Modus:</strong> Bei Serien zusätzlich "nur diese Folge" oder "alle Folgen".<br><br>
      <strong>Regex-Pattern</strong> bestimmt welche EPG-Titel aufgenommen werden (case-insensitive):<br>
      <code>Die Geissens</code> alle Folgen &bull;
      <code>GNTM|Germany.s Next Topmodel</code> beide Schreibweisen &bull;
      <code>^Tatort</code> nur Titel die mit Tatort beginnen<br><br>
      <strong>Pre/Post-Offset (Sek.):</strong> Pro Serie konfigurierbar. Manche Sender starten ihre Sendungen
      regelmäßig 30s früher oder enden später. Positive Werte = früher starten / länger laufen.<br><br>
      <strong>Back-to-Back:</strong> Folgt eine Aufnahme direkt auf eine andere auf dem gleichen Sender
      (≤60s Lücke), startet sie automatisch 2 Minuten früher — vermeidet Time-Drift.<br><br>
      <strong>Keep Last:</strong> Behält nur die letzten N Aufnahmen, ältere werden automatisch gelöscht (inkl. .nfo). 0 = alle.<br><br>
      <strong>TMDB-Suche:</strong> Liefert deutschen Titel, Aliases und Regex-Vorschlag.<br><br>
      <strong>Duplikat-Schutz:</strong> Gleicher Name + Sender kann nicht doppelt angelegt werden.</p>
    </div>

    <div class="card">
      <div class="card-title" data-i18n="help.h_recordings">Aufnahmen</div>
      <p data-i18n-html="help.recordings">Sortierbar nach Datum, Serie, Titel, Status. Filterbar nach Status.<br><br>
      <strong>Status:</strong> Geplant · <span style="color:var(--accent2)">Läuft</span> · <span style="color:var(--green)">Fertig</span> · <span style="color:var(--red)">Fehler</span> · <span style="color:var(--yellow)">Verpasst</span> · Ausgelassen<br><br>
      <strong>&#128269; Detail:</strong> Dateipfad, Größe, Proxy, Quelle (epg-scheduler / ui-quickrecord).<br><br>
      <strong>&#9654; Play:</strong> Aufnahme im Browser abspielen (benötigt <code>/recording/stream</code> im e2proxy).<br><br>
      <strong>Rescan:</strong> Prüft Proxy-Status + Dateisystem, aktualisiert fehlende Dateien.<br><br>
      <strong>Fehler löschen:</strong> Entfernt alle Einträge mit Status Fehler, Verpasst, Ausgelassen und fehlenden Dateien.</p>
    </div>

    <div class="card">
      <div class="card-title" data-i18n="help.h_settings">Settings</div>
      <p data-i18n-html="help.settings"><strong>Proxies:</strong> SSDP-Discovery oder manuelle URL. Mehrere Proxies möglich — der mit den meisten freien Tunern wird automatisch gewählt.<br><br>
      <strong>Stream-Profil:</strong> Wird aus e2proxy geladen. Empfohlen: <code>remux-ac3</code>.<br><br>
      <strong>Pre/Post-Buffer:</strong> Sekunden vor/nach EPG-Zeit für ungenaue Sendezeiten.<br><br>
      <strong>Aufräum-Trigger:</strong> "Bei neuer Aufnahme" empfohlen.<br><br>
      <strong>TMDB API Key:</strong> Kostenlos unter themoviedb.org.<br><br>
      <strong>API-Call Logging:</strong> Loggt jeden Request/Response zum e2proxy — nur für Troubleshooting, kombinieren mit Log-Level DEBUG.</p>
    </div>

    <div class="card grid-full">
      <div class="card-title" data-i18n="help.h_tech">Technische Details &amp; Raspberry Pi Hinweise</div>
      <p data-i18n-html="help.tech"><strong>Aufnahme-Lifecycle:</strong> EPG-Scan → Regex-Match → "Geplant". Kurz vor Sendestart: Proxy mit freiem Tuner wählen → <code>POST /api/record/start</code> → Dateipfad aus Response. Watchdog alle 30s via <code>GET /api/record/status</code> → bei Fertig: expliziter Stop für Tuner-Freigabe.<br><br>
      <strong>Kein eigenes ffmpeg:</strong> Alle Aufnahmen laufen im e2proxy. Speicherort wird vom e2proxy bestimmt (<code>recordings_path</code>).<br><br>
      <strong>Daten:</strong> <code>~/e2recorder/data/</code> — config.json, series.json, recordings.json, tuner_history.json.<br><br>
      <strong style="color:var(--red)">&#9888; Raspberry Pi — Netzteil &amp; Kabel:</strong>
      Unter Last kann der Pi bei schlechter Stromversorgung einfrieren (Symptom: <code>hwmon: Undervoltage detected!</code> in dmesg).<br>
      &bull; <strong>Netzteil:</strong> Mindestens 27W USB-C (5V/5A) — z.B. Anker Nano II 65W &#10003;<br>
      &bull; <strong>Kabel:</strong> Kurzes, dickes USB-C Kabel — dünne/lange Kabel verursachen Spannungsabfall unter Last<br>
      Prüfen: <code>vcgencmd get_throttled</code> → sollte <code>0x0</code> zurückgeben</p>
    </div>

    <div class="card grid-full">
      <div class="card-title" data-i18n="help.h_changelog">Changelog</div>

      <div class="cl-entry cl-current">
        <b style="font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--accent2)">v1.4.1</b>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-16</span>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">Hilfe als Tab · Sprache in Einstellungen</span>
        <div style="font-size:11px;margin-top:4px;color:var(--muted)">
          Hilfe &amp; Changelog jetzt als integrierter Tab in der gleichen Seite (kein separates Fenster mehr) — gleiches Design.
          Sprachauswahl in die Einstellungen verschoben (wie im e2proxy), direkt unter der Theme-Auswahl.
        </div>
      </div>

      <div class="cl-entry cl-old">
        <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.4.0</b>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-16</span>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">Mehrsprachige UI (EN/DE)</span>
        <div style="font-size:11px;margin-top:4px;color:var(--muted)">
          Komplette WebUI jetzt zweisprachig (Englisch/Deutsch) mit clientseitiger i18n ohne Server-Roundtrip.
          Auswahl wird in localStorage gespeichert. Hilfe-Seite ebenfalls übersetzt.
        </div>
      </div>

      <div class="cl-entry cl-old">
        <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.3.2</b>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-16</span>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">Fix: "Auslassen"-Button erscheint wieder</span>
        <div style="font-size:11px;margin-top:4px;color:var(--muted)">
          Behoben: Bei Serien mit Doppelpunkt-Sender-Ref (z.B. First Dates) fehlte der "Diese Aufnahme auslassen"-Button im EPG-Tooltip.
          Ursache war ein Format-Mismatch (':' vs. '_') im internen Event-Key — jetzt normalisiert.
        </div>
      </div>

      <div class="cl-entry cl-old">
        <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.3.1</b>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-16</span>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">Tooltip-Fix · Auto-Close · Button-Text</span>
        <div style="font-size:11px;margin-top:4px;color:var(--muted)">
          EPG-Tooltip wird jetzt korrekt im Viewport positioniert — der "Auslassen"-Button bleibt auch bei langer Beschreibung sichtbar,
          Quick-Record Dialog schließt sich automatisch nach dem Einplanen,
          Button-Text generisch "Diese Aufnahme auslassen" (auch für Filme)
        </div>
      </div>

      <div class="cl-entry cl-old">
        <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.3.0</b>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-16</span>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">Getrennte Tabs Serien/Filme · Cache-Control</span>
        <div style="font-size:11px;margin-top:4px;color:var(--muted)">
          Eigene Tabs für Serien und Filme (statt kombiniert mit Filter),
          "+ Neu" öffnet jeweils den passenden Typ vorausgewählt,
          Cache-Control Header für die UI (kein veraltetes UI mehr aus dem Browser-Cache)
        </div>
      </div>

      <div class="cl-entry cl-old">
        <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.2.0</b>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-15</span>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">Filme/Serien · Pre/Post-Offsets · Back-to-Back · Channel-Fix</span>
        <div style="font-size:11px;margin-top:4px;color:var(--muted)">
          Movie/Series Typ-Auswahl im Quick-Record Modal,
          Pre-/Post-Offset Sekunden pro Serie konfigurierbar (für Sender die früher/später anfangen),
          Back-to-Back Erkennung auf gleichem Sender → 2 Min früher starten,
          Filter "Nur Serien"/"Nur Filme" im Serien-Tab,
          Channel-Selector Fix (Refs mit _ und : werden gematcht),
          year-Feld für Filme (für Plex Movie-Titel),
          erweiterte e2proxy v3.2 API mit kind/season/episode
        </div>
      </div>

      <div class="cl-entry cl-old">
        <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.1.0</b>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-10</span>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">/api/health · Receiver Tracking · File Logging · Intelligentes Tuner-Wait</span>
        <div style="font-size:11px;margin-top:4px;color:var(--muted)">
          /api/health für schnellere Proxy-Erkennung, Receiver-Tracking,
          Intelligentes Warten via remaining_sec, File-Logging mit täglicher Rotation,
          API-Call Logging Toggle, einmalige Aufnahmen (once=True)
        </div>
      </div>

      <div class="cl-entry cl-old">
        <b style="font-family:'JetBrains Mono',monospace;font-size:11px">v1.0.0</b>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">2026-06-09</span>
        <span style="color:var(--muted);font-size:10px;margin-left:8px">Erster stabiler Release · Docker · Recording · EPG · UI</span>
        <div style="font-size:11px;margin-top:4px;color:var(--muted)">
          Serien-Aufnahme-Scheduler via e2proxy API, EPG-Grid, TMDB, SSDP-Discovery,
          Multi-Proxy, Keep-Last-Cleanup, Docker (python:3.11-slim), Tuner-History,
          Duplikat-Schutz für Serien, Netzteil/Kabel Warnung
        </div>
      </div>

    </div>

  </div>
</div>
</body>
</html>"""


def render_ui():
    return _HTML.replace("{CSS}", _CSS).replace("{I18N}", I18N_JS).replace("{VERSION}", VERSION).replace("{JS}", _JS)


# ── HTTP Handler ───────────────────────────────────────────────────────────

LOG_LEVELS = {"DEBUG":0,"INFO":1,"WARNING":2,"ERROR":3}

def _json(handler, data, status=200):
    body = json.dumps(data, ensure_ascii=False, default=str).encode()
    handler.send_response(status)
    handler.send_header("Content-Type","application/json; charset=utf-8")
    handler.send_header("Content-Length", len(body))
    handler.send_header("Access-Control-Allow-Origin","*")
    handler.end_headers()
    handler.wfile.write(body)

def _html(handler, html):
    body = html.encode('utf-8', errors='replace')
    handler.send_response(200)
    handler.send_header("Content-Type","text/html; charset=utf-8")
    handler.send_header("Content-Length", len(body))
    handler.send_header("Cache-Control","no-cache, no-store, must-revalidate")
    handler.send_header("Pragma","no-cache")
    handler.send_header("Expires","0")
    handler.end_headers()
    handler.wfile.write(body)

def _read(handler):
    n = int(handler.headers.get("Content-Length",0))
    return json.loads(handler.rfile.read(n)) if n else {}


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,*a): pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","GET,POST,PUT,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers","Content-Type")
        self.end_headers()

    def do_GET(self):
        p    = urllib.parse.urlparse(self.path)
        path = p.path.rstrip("/")
        qs   = dict(urllib.parse.parse_qsl(p.query))

        if path in ("","/"): _html(self, render_ui()); return

        if path == "/help": _html(self, render_help()); return

        if path == "/api/version":
            _json(self, {"version": VERSION, "service": "e2recorder"}); return

        if path == "/api/status":
            recs = get_recordings()
            counts = {}
            for r in recs:
                s = r.get("status","?"); counts[s] = counts.get(s,0)+1
            online = sum(1 for p2 in get_proxies() if p2.get("enabled",True) and fetch_proxy_status(p2["url"]))
            _json(self, {
                "ok": True,
                "series_count":  len(get_series()),
                "recordings":    counts,
                "proxies_online": online,
                "discovery":     _discovery_status,
                "scan_status":   _scan_status,
            }); return

        if path == "/api/config":   _json(self, get_config_dict()); return
        if path == "/api/series":   _json(self, get_series()); return
        if path == "/api/recordings": _json(self, get_recordings()); return

        m = re.match(r"^/api/recordings/([^/]+)/detail$", path)
        if m:
            rec = get_rec_by_id(m.group(1))
            if not rec:
                _json(self, {"error": "not found"}, 404); return
            fp = rec.get("filepath", "")
            detail = dict(rec)
            detail["file_exists"] = bool(fp and os.path.exists(fp))
            if detail["file_exists"]:
                try: detail["filesize"] = os.path.getsize(fp)
                except Exception: pass
            proxy_url = rec.get("proxy_url") or get_proxy_url()
            if proxy_url and fp:
                detail["stream_url"] = f"{proxy_url}/recording/stream?file={urllib.parse.quote(fp)}"
            else:
                detail["stream_url"] = ""
            _json(self, detail); return

        if path == "/api/channels": _json(self, fetch_channels()); return
        if path == "/api/proxies":  _json(self, get_all_proxy_statuses()); return

        if path == "/api/schedule":
            with _epg_cache_lock:
                cached = list(_epg_cache)
            # Cache leer? Sofort laden (blocking aber einmalig)
            if not cached:
                cached = get_cached_epg()
            series_list = [s for s in get_series() if s.get("enabled",True)]
            now_ts    = time.time()
            lookahead = cfg("epg_lookahead_hours") * 3600
            result = []
            for ch in cached:
                ch_id   = ch.get("id","")
                ch_name = ch.get("name","")
                ch_series = [s for s in series_list
                             if refs_match(s["channel_ref"], ch_id) or s["channel_name"]==ch_name]
                events_out = []
                for ev in ch.get("events",[]):
                    start_ts = ev.get("start",0); stop_ts = ev.get("stop",0)
                    if stop_ts < now_ts or start_ts > now_ts + lookahead: continue
                    title   = ev.get("title","")
                    matched = next((s for s in ch_series if title_matches(title,s)), None)
                    rec_id  = None
                    if matched:
                        ekey = _event_key(ch_id, start_ts)
                        with _scheduled_lock:
                            r2 = _scheduled.get(ekey)
                            if r2: rec_id = r2.get("id")
                    events_out.append({
                        "start": start_ts, "stop": stop_ts,
                        "title": title, "subtitle": ev.get("sub",""), "desc": ev.get("desc",""),
                        "matched": matched is not None,
                        "serie_id":   matched["id"]   if matched else None,
                        "serie_name": matched["name"] if matched else None,
                        "rec_id":     rec_id,
                    })
                if events_out:
                    # Sicherstellen dass channel_name nie leer ist
                    display_name = ch_name or ch_id
                    result.append({"channel_id":ch_id,"channel_name":display_name,
                                   "logo":ch.get("logo",""),"events":events_out})
                else:
                    # Sender ohne EPG-Daten (z.B. gerade neu hinzugefügt) trotzdem
                    # anzeigen, damit er im Plan sichtbar ist.
                    display_name = ch_name or ch_id
                    result.append({"channel_id":ch_id,"channel_name":display_name,
                                   "logo":ch.get("logo",""),"events":[],"no_epg":True})
            _json(self, result); return

        if path == "/api/tmdb/search":
            q = qs.get("q","").strip()
            _json(self, tmdb_search(q) if q else {"error":"Kein Suchbegriff"}, 200 if q else 400); return

        if path == "/api/logs":
            lvl  = qs.get("level","INFO").upper()
            min_ = LOG_LEVELS.get(lvl,1)
            out  = [e for e in _LOG_BUFFER if LOG_LEVELS.get(e["level"],0) >= min_]
            _json(self, list(out)[-200:]); return

        if path == "/api/logs/files":
            log_dir = os.path.join(DATA_DIR, "logs")
            files = []
            if os.path.isdir(log_dir):
                for fn in sorted(os.listdir(log_dir), reverse=True):
                    fp = os.path.join(log_dir, fn)
                    if os.path.isfile(fp):
                        files.append({
                            "name": fn,
                            "size": os.path.getsize(fp),
                            "modified": datetime.fromtimestamp(
                                os.path.getmtime(fp)).isoformat()
                        })
            _json(self, files); return

        if path == "/api/tuner/history":
            try:
                with open(HISTORY_FILE) as f:
                    history = json.load(f)
                _json(self, list(reversed(history[-50:])))
            except Exception:
                _json(self, [])
            return

        self.send_response(404); self.end_headers()

    def do_POST(self):
        p    = urllib.parse.urlparse(self.path)
        path = p.path.rstrip("/")

        if path == "/api/series":
            b = _read(self)
            s = add_serie(
                name=b.get("name",""), channel_name=b.get("channel_name",""),
                channel_ref=b.get("channel_ref",""), keep_last=int(b.get("keep_last",0)),
                enabled=bool(b.get("enabled",True)), regex_pattern=b.get("regex_pattern",""),
                tmdb_id=b.get("tmdb_id"), tmdb_poster=b.get("tmdb_poster",""),
            )
            _json(self, s, 201); return

        if path == "/api/series/from-epg":
            b = _read(self)
            title   = b.get("title",""); ch_name = b.get("channel_name",""); ch_ref = b.get("channel_ref","")
            if not title or not ch_ref: _json(self,{"error":"title/channel_ref fehlt"},400); return
            existing_check = next(
                (x for x in get_series()
                 if x.get("channel_ref") == ch_ref and x.get("name","").lower() == title.lower()),
                None
            )
            is_once = b.get("once", True)
            kind = b.get("kind")  # "movie" | "series" | None
            s = add_serie(
                name=title, channel_name=ch_name, channel_ref=ch_ref,
                regex_pattern=b.get("regex_pattern", re.escape(title)),
                once=is_once,
                once_start_ts=b.get("start_ts") if is_once else None,
                kind=kind,
                year=b.get("year"),
            )
            is_duplicate = existing_check is not None
            start_ts = b.get("start_ts",0); stop_ts = b.get("stop_ts",0)
            rec = None
            if start_ts and stop_ts and start_ts > time.time() - cfg("pre_buffer_sec"):
                fp  = _output_path(s, title, start_ts)
                key = _event_key(ch_ref, start_ts)
                rec = {"id":str(uuid.uuid4())[:8],"serie_id":s["id"],"serie_name":s["name"],
                       "title":title,"subtitle":b.get("subtitle",""),"desc":"",
                       "channel_name":ch_name,"channel_ref":ch_ref,
                       "start_ts":start_ts,"stop_ts":stop_ts,"filepath":fp,
                       "status":"scheduled","protected":False,"proxy_url":None,
                       "proxy_rec_id":None,"tmdb_poster":"","created_at":datetime.now().isoformat()}
                with _scheduled_lock: _scheduled[key] = rec
                upsert_rec(rec)
            else:
                threading.Thread(target=run_epg_scan,kwargs={"manual":True},daemon=True).start()
            _json(self,{"ok":True,"serie":s,"recording":rec,"duplicate":existing_check is not None},201); return

        if path == "/api/config":
            update_config(_read(self)); _json(self,{"ok":True}); return

        if path == "/api/scan":
            threading.Thread(target=run_epg_scan,kwargs={"manual":True},daemon=True).start()
            _json(self,{"ok":True}); return

        if path == "/api/discover":
            threading.Thread(target=run_discovery,daemon=True).start()
            _json(self,{"ok":True}); return

        if path == "/api/cleanup":
            threading.Thread(target=cleanup_all_series,daemon=True).start()
            _json(self,{"ok":True}); return

        if path == "/api/log/level":
            b = _read(self)
            level_str = b.get("level","INFO").upper()
            if level_str not in ("DEBUG","INFO","WARNING","ERROR"):
                _json(self,{"error":"Ungültiger Level"},400); return
            update_config({"log_level": level_str})
            _apply_log_level()
            log.info(f"Log-Level geändert auf: {level_str}")
            _json(self,{"ok":True,"level":level_str}); return

        if path == "/api/admin/deduplicate":
            count = _deduplicate_recordings_db()
            _json(self, {"ok": True, "removed": count}); return

        if path == "/api/admin/rescan":
            # Synchron ausführen (kurz blockierend aber für UI-Feedback nötig)
            updated = _rescan_recordings()
            _json(self, {"ok": True, "msg": f"{updated} Eintraege aktualisiert", "updated": updated}); return

        if path == "/api/proxies":
            b = _read(self)
            p2 = add_or_update_proxy(b.get("url",""), b.get("name",""), b.get("enabled",True))
            _json(self, p2); return

        if path == "/api/proxies/remove":
            remove_proxy(_read(self).get("url",""))
            _json(self,{"ok":True}); return

        m = re.match(r"^/api/recordings/([^/]+)/keep$", path)
        if m:
            rid=m.group(1); rec=get_rec_by_id(rid)
            if rec: rec["protected"]=_read(self).get("protected",True); upsert_rec(rec); _json(self,rec)
            else:   _json(self,{"error":"not found"},404)
            return

        self.send_response(404); self.end_headers()

    def do_PUT(self):
        path = urllib.parse.urlparse(self.path).path.rstrip("/")
        m = re.match(r"^/api/series/([^/]+)$", path)
        if m:
            u = update_serie(m.group(1), _read(self))
            _json(self, u if u else {"error":"not found"}, 200 if u else 404); return
        self.send_response(404); self.end_headers()

    def do_DELETE(self):
        path = urllib.parse.urlparse(self.path).path.rstrip("/")

        m = re.match(r"^/api/series/([^/]+)$", path)
        if m: _json(self,{"ok":delete_serie(m.group(1))}); return

        m = re.match(r"^/api/schedule/([^/]+)$", path)
        if m:
            rid=m.group(1); rec=get_rec_by_id(rid)
            if rec:
                key = _event_key(rec.get("channel_ref",""), rec.get("start_ts",0))
                with _scheduled_lock: _scheduled.pop(key,None)
                rec["status"]="skipped"; rec["skipped_at"]=datetime.now().isoformat()
                upsert_rec(rec); _json(self,{"ok":True})
            else: _json(self,{"error":"not found"},404)
            return

        m = re.match(r"^/api/recordings/([^/]+)$", path)
        if m:
            rid=m.group(1); rec=get_rec_by_id(rid)
            if rec:
                # Laufende Aufnahme am Proxy stoppen falls nötig
                if rec.get("status") == "recording" and rec.get("proxy_url") and rec.get("proxy_rec_id"):
                    threading.Thread(target=_stop_proxy_recording,
                        args=(rec["proxy_url"], rec["proxy_rec_id"]),daemon=True).start()
                # Nur DB-Eintrag löschen — Datei auf Disk bleibt erhalten
                delete_rec_entry(rid); _json(self,{"ok":True})
            else: _json(self,{"error":"not found"},404)
            return

        self.send_response(404); self.end_headers()


# ── Main ────────────────────────────────────────────────────────────────────

def _cleanup_stale_scheduled():
    """
    Beim Start: verpasste Aufnahmen korrekt markieren.
    - 'scheduled' + Sendezeit vorbei → 'missed'
    - 'recording' + keine aktive Aufnahme am Proxy → 'done' oder 'failed' prüfen
    """
    now_ts = time.time()
    with _rec_lock:
        data = _load_recs()
        changed = False
        for rec in data:
            status = rec.get("status")
            stop_ts = rec.get("stop_ts", 0)

            # Geplante Aufnahmen deren Zeit vorbei ist
            if status == "scheduled" and stop_ts < now_ts - 300:
                rec["status"]     = "missed"
                rec["skipped_at"] = datetime.now().isoformat()
                rec["error"]      = "Sendung verpasst — Service war gestoppt"
                changed = True
                log.warning(f"Verpasst: '{rec.get('title','')}' am "
                            f"{datetime.fromtimestamp(rec.get('start_ts',0)).strftime('%d.%m. %H:%M')}")

            # Aufnahmen die als 'recording' hängen geblieben sind
            elif status == "recording" and stop_ts < now_ts - 600:
                proxy_url    = rec.get("proxy_url")
                proxy_rec_id = rec.get("proxy_rec_id")
                if proxy_url and proxy_rec_id:
                    running, _, status_data = _proxy_recording_running(proxy_url, proxy_rec_id)
                    if running is False:
                        # Am Proxy nicht mehr aktiv → als done markieren
                        rec["status"]      = "done"
                        rec["finished_at"] = datetime.now().isoformat()
                        if status_data and status_data.get("filename"):
                            dirpath = os.path.dirname(rec.get("filepath", ""))
                            rec["filepath"] = os.path.join(dirpath, status_data["filename"])
                        changed = True
                        log.info(f"Aufnahme nachträglich als fertig markiert: '{rec.get('title','')}'")
                    elif running is None:
                        # Proxy nicht erreichbar, unbekannt
                        rec["status"] = "unknown"
                        changed = True
                else:
                    # Kein Proxy bekannt → failed
                    rec["status"] = "failed"
                    rec["error"]  = "Proxy-Verbindung nach Neustart verloren"
                    changed = True

        if changed:
            _save_recs(data)


def _restore_scheduled_from_db():
    """Stellt _scheduled Dict aus DB wieder her — verhindert Duplikate nach Restart."""
    now_ts = time.time()
    restored = 0
    with _rec_lock:
        recs = _load_recs()
    for rec in recs:
        if rec.get("status") not in ("scheduled", "recording"):
            continue
        # Vergangene Sendungen ignorieren
        if rec.get("stop_ts", 0) < now_ts:
            continue
        key = _event_key(rec.get("channel_ref", ""), rec.get("start_ts", 0))
        with _scheduled_lock:
            if key not in _scheduled:
                _scheduled[key] = rec
                restored += 1
    if restored:
        log.info(f"Scheduled aus DB wiederhergestellt: {restored} Einträge")


def run():
    os.makedirs(DATA_DIR, exist_ok=True)
    load_config()
    _setup_file_logging()
    port = int(cfg("recorder_port"))

    # DB bereinigen
    _deduplicate_series_db()
    removed = _deduplicate_recordings_db()
    if removed:
        log.info(f"Startup-Dedup: {removed} Duplikate bereinigt")
    # _scheduled aus DB laden bevor Threads starten
    _restore_scheduled_from_db()
    # Alte vergangene "scheduled" Einträge als skipped/missed markieren + Proxy prüfen
    _cleanup_stale_scheduled()
    # Dateigrößen und fehlende Dateien prüfen (im Hintergrund)
    threading.Thread(target=_rescan_recordings, daemon=True).start()

    threading.Thread(target=discovery_scheduler,     daemon=True).start()
    threading.Thread(target=epg_scan_scheduler,      daemon=True).start()
    threading.Thread(target=recording_dispatcher,    daemon=True).start()
    threading.Thread(target=daily_cleanup_scheduler, daemon=True).start()

    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), Handler)

    log.info("=" * 55)
    log.info(f"e2recorder v{VERSION} gestartet")
    log.info(f"Web-UI:  http://0.0.0.0:{port}/")
    log.info(f"Daten:   {DATA_DIR}")
    log.info("=" * 55)

    def _stop(sig, frame):
        log.info("Stoppe...")
        threading.Thread(target=server.shutdown, daemon=True).start()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    run()
