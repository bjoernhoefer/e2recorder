# e2recorder

**Series Recording Scheduler for e2proxy** — Automatically records TV series and movies from Enigma2 receivers by monitoring the EPG and delegating every recording to [e2proxy](https://github.com/bjoernhoefer/e2proxy).

Single Python file, runs in Docker, no own ffmpeg. It scans the EPG, matches your series via regex, and triggers recordings through the e2proxy `/api/record/start` endpoint — picking the proxy with the most free tuners.

## Features

- **Series Scheduler** — Define series once via regex, every matching episode is recorded automatically
- **Movies & Series** — Explicit `kind` (movie/series) for Plex-compliant paths and `.nfo` metadata
- **EPG Browser** — Interactive timeline grid with one-click recording, "skip this episode", and TMDB artwork
- **Quick Record** — Record a single episode, a one-off movie, or set up a recurring series straight from the EPG
- **Smart Tuner Management** — Waits for your own recordings to finish (via `remaining_sec`), never fights its own tuners
- **Multi-Proxy** — SSDP auto-discovery, automatically selects the proxy with the most free tuners
- **Per-Series Offsets** — Start earlier / run longer per series for channels that air early or late
- **Back-to-Back Detection** — Consecutive recordings on the same channel start 2 min early to avoid time drift
- **Keep Last N** — Automatic cleanup of old episodes (including `.nfo`), keeping only the newest N
- **TMDB Search** — German titles, aliases, and regex suggestions when adding a series
- **File Logging** — Daily-rotated logs with configurable retention
- **Dark/Light Theme** — Switchable in settings, shared look with e2proxy

## Quick Start

### Docker (recommended)

```yaml
# docker-compose.e2recorder.yml
services:
  e2recorder:
    image: python:3.11-slim
    container_name: e2recorder
    restart: unless-stopped
    network_mode: host          # required for SSDP discovery + LAN access
    volumes:
      - ./e2recorder.py:/app/e2recorder.py:ro
      - ./data:/data
      - /mnt/nvme/recordings:/mnt/nvme/recordings
    working_dir: /app
    command: python3 e2recorder.py
    environment:
      - PYTHONUNBUFFERED=1
      - E2REC_DATA_DIR=/data
    healthcheck:
      test: ["CMD", "python3", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8889/api/status', timeout=3)"]
      interval: 30s
      timeout: 5s
      retries: 3
```

```bash
docker compose -f docker-compose.e2recorder.yml up -d
```

Open http://your-server:8889 and add your e2proxy instance in Settings (or let SSDP discovery find it).

No Dockerfile, no build — the `python:3.11-slim` image runs the single script mounted read-only, so updates are just a file swap and a restart.

### Requirements

- Python 3.11+ (no external dependencies — standard library only)
- A running [e2proxy](https://github.com/bjoernhoefer/e2proxy) instance (v3.2.0+) on the local network
- `network_mode: host` (required for SSDP UDP multicast on port 1900)

## Architecture

```
┌──────────────┐
│   Browser    │
│  EPG / UI    │
└──────┬───────┘
       │  HTTP :8889
┌──────┴────────────────────────────┐
│            e2recorder              │
│  ┌──────────┐   ┌───────────────┐  │
│  │ EPG Scan │   │  Scheduler /  │  │
│  │ (regex)  │──▶│  Tuner Wait   │  │
│  └──────────┘   └───────┬───────┘  │
└─────────────────────────┼──────────┘
                          │  POST /api/record/start
                  ┌───────┴────────┐
                  │    e2proxy     │
                  │   HTTP :8888   │
                  │ ffmpeg + tuners│
                  └───────┬────────┘
              ┌───────────┴───────────┐
        ┌─────┴──────┐         ┌──────┴─────┐
        │ Receiver 1 │         │ Receiver 2 │
        │ (Enigma2)  │         │ (Enigma2)  │
        └────────────┘         └────────────┘
```

e2recorder never writes recordings itself — it decides **what** and **when**, e2proxy decides **where** and does the actual ffmpeg work. The recording file path always comes from the e2proxy response (`resp["file"]`).

## Configuration

First-time setup via the web UI at `http://your-server:8889` → Settings:

1. **Proxies** — SSDP discovery or manual URL. Multiple proxies supported — the one with the most free tuners is chosen automatically.
2. **Stream Profile** — Loaded from e2proxy. Recommended: `remux-ac3`.
3. **Pre/Post Buffer** — Seconds before/after EPG time to cover inaccurate broadcast times.
4. **EPG Lookahead** — How far ahead to scan (default 72 h).
5. **TMDB API Key** — Optional, for poster artwork and German title/regex suggestions.
6. **Cleanup** — Trigger on every new recording (`on_new`) or daily at a fixed hour.

All configuration is stored in `/data/config.json`.

## Web UI

| Tab | Description |
|-----|-------------|
| overview | EPG timeline grid — click a slot to record, hover for details and "skip" |
| serien | Manage series — regex pattern, channel, keep-last, per-series offsets |
| filme | Manage movies — same form, written to the Plex Movies library |
| aufnahmen | Recordings list — sortable/filterable by status, with detail and playback |
| settings | Proxies, stream profile, buffers, EPG, cleanup, logs |
| /help | Feature overview and changelog |

## API

### Series & Schedule

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/series` | GET/POST | List / add a series |
| `/api/series/<id>` | PUT/DELETE | Edit / delete a series |
| `/api/series/from-epg` | POST | Create a series or one-off recording from an EPG click |
| `/api/schedule` | GET | Recording plan (EPG events + matches) |
| `/api/schedule/<id>` | DELETE | Skip a single scheduled recording |
| `/api/scan` | POST | Trigger an EPG scan |

### Recordings

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/recordings` | GET | All recordings |
| `/api/recordings/<id>/detail` | GET | File path, size, proxy, stream URL |
| `/api/recordings/<id>/keep` | POST | Protect a recording from cleanup |
| `/api/recordings/<id>` | DELETE | Delete the DB entry (file stays on disk) |
| `/api/cleanup` | POST | Run keep-last cleanup for all series |

### Proxies & Channels

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/proxies` | GET/POST | List / add-update proxies (+ tuner status) |
| `/api/proxies/remove` | POST | Remove a proxy |
| `/api/discover` | POST | SSDP discovery |
| `/api/channels` | GET | Channel list (e2proxy favorites) |
| `/api/tmdb/search?q=` | GET | TMDB search |

### System

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/version` | GET | Version + build ID |
| `/api/status` | GET | Service status (series, recordings, proxies online) |
| `/api/config` | GET/POST | Configuration |
| `/api/logs` | GET | Live logs (`?level=INFO`) |
| `/api/logs/files` | GET | Rotated log files on disk |
| `/api/tuner/history` | GET | Last 200 tuner decisions |
| `/api/log/level` | POST | Change log level at runtime |

## How It Works

1. A series has a regex pattern (and optionally a TMDB id), a channel, and a `kind` (movie/series).
2. The EPG scanner (hourly + on demand) matches upcoming events against your series.
3. A match becomes a `scheduled` recording, with per-series pre/post offsets applied.
4. At start time, e2recorder calls `POST /api/record/start` on the best proxy:

   ```json
   {
     "ref": "1_0_19_EF11_421_1_C00000_0_0_0_",
     "title": "First Dates",
     "episode_title": "Ein Tisch für zwei",
     "kind": "series",
     "season": 1,
     "episode": 3,
     "duration": 3600,
     "profile": "remux-ac3"
   }
   ```

5. e2proxy builds the Plex path, runs ffmpeg, writes the `.nfo`, and refreshes Plex. The file path comes from `resp["file"]` — e2recorder never computes it.
6. When all tuners are busy, e2recorder distinguishes its **own** recordings (waits using `remaining_sec`) from foreign tuner usage.

## Recording Structure

e2proxy (v3.2) decides the structure from `kind`, writing everything under a single `recordings_path`:

```
/mnt/nvme/recordings/
├── Movies/
│   └── Shooter (2007)/
│       ├── Shooter (2007).ts
│       └── Shooter (2007).nfo
└── TV/
    └── First Dates/
        ├── Season 01/
        │   ├── First Dates - S01E03 - Ein Tisch für zwei.ts
        │   └── First Dates - S01E03 - Ein Tisch für zwei.nfo
        └── tvshow.nfo
```

- **Series with TVDB/explicit S/E** → real season/episode numbers
- **Daily shows without S/E** → day-of-year fallback (`S2026E163` = June 12)
- **Movies** → `Movies/<Title> (<Year>)/`

## Companion: e2proxy

[e2proxy](https://github.com/bjoernhoefer/e2proxy) is the Enigma2 streaming proxy that does the actual streaming and recording. e2recorder is the scheduler on top of it — they are designed to run side by side (e2proxy on `:8888`, e2recorder on `:8889`).

## Data Paths

| Path | Content |
|------|---------|
| `/data/config.json` | Configuration |
| `/data/series.json` | Series & movie definitions |
| `/data/recordings.json` | Recording records |
| `/data/tuner_history.json` | Last 200 tuner decisions |
| `/data/logs/e2recorder.log` | System log (daily rotation) |

## Update

```bash
# Copy new version and restart
scp e2recorder.py user@server:~/e2recorder/
docker compose -f ~/e2recorder/docker-compose.e2recorder.yml restart

# Verify
curl -s http://server:8889/api/version
```

## License

MIT

## Credits

Built with [Claude](https://claude.ai) by Anthropic.
