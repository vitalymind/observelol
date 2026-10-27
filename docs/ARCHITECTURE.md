# Project infrastructure plan

## Producers
Producers POST to /api/event/ and GET from /api/new-game/ via REST API.

## Rust Dispatcher (VM1)
Rust dispatcher is the public HTTPS entry point on port 443:
- terminates TLS and maintains persistent HTTP/1.1 & HTTP/2 keep-alive connections.
- accepts all `/api/event/` events answering with 200, stripping `game_id` from the payload and appending event to Redis (RPUSH to `game:{game_id}:events`) with safety TTL.
- generates `game_id` (`UUIDv7`) in-memory on `/api/new-game/` and provides it along with item/hero snapshot (read from Redis) to Producers.
- answers `/health` directly by pinging Redis.
- checks event type and on game end, spawns a grace timeout with `XADD ready_games * game_id {id}` payload.
- forwards all other traffic to the internal nginx container.

## Nginx (VM1)
Sits behind the Rust dispatcher and handles non-ingest traffic destined for the VM2 `control` app (currently returning a static `ok` response).

## Redis (VM1)
Stores events (RPUSH) and manages Consumer Groups of workers.

## PostgreSQL (VM2)
Postgre is running in own container on VM2

## Persistent detachable disc (Connected to VM2)
Contains DB and other files maintained by `control`

## React + Next.js app (VM2)
Nodejs with Nextjs `control` running in container on VM2, its job is to process admin requests (react + next.js), keep track of replay files, periodically bundle replay files into CDN. `control` does not handle significant traffic and serves as control plane for administrator users. `control` writes to Redis when item/hero snapshot changes.

## Workers (VM3)
Stateless containers running Rust apps with following functionlaity
- Register with Redis (XREADGROUP) to wait for ready_games BLOCKING
- Grab (lrange) all events for a game
- Do de-duplication using message index
- Check consistency, no missing indexes
- Run game checks, heroes set to slots, start game, start round.
- If events check failed, issue error to DB (`ON CONFLICT (game_id) DO NOTHING`).
- If check succeed, compile replay and save to DB (`ON CONFLICT (game_id) DO NOTHING`).
- Pipeline release events from Redis (`UNLINK`) and ack/clear game from `ready_games` (`XACK` + `XDEL`) together after the DB write.
- Check if Redis have any abandoned games (XAUTOCLAIM)
Workers might get empty list of events (duplicated end game event), in wich case it will be ignored and worker skips silently doin nothing.

## OpenTelemetry + lgtm stack
otel-lgtm container (VM2) stream telemetry data, grafana is accessible via gcloud CLI (gCloud tunnel).
Both VM1 and VM3, running prom/node-exporter container to report to hostmetrics on VM2


## Backups
It is yet to be decided what backup strategy to use Logical Backups or Physical + Continuous WAL.
