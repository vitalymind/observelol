# Project infrastructure plan

## Producers
Producers POST to /api/event/ and GET from /api/new-game/ via REST API.

## Nginx (VM1)
Traffic injested by nginx container connected via unix sockets to Rust dispatcher. Other then events traffic is routed to VM2 `control` app.

## Rust Dispatcher (VM1)
Rust dispatcher doing following:
- accepts all events answering with 200, stripping `game_id` from the payload and appending event to Redis (RPUSH to `game:{game_id}:events`) with safety TTL.
- generates `game_id` (`UUIDv7`) in-memory and provides it along with item/hero snapshot (read from Redis) to Producers.
- will check event type and on game end, it will spawn 1 second timeout with XADD ready_games * game_id {id} payload.

## Redis (VM1)
Stores events (RPUSH) and manages Consumer Groups of workers.

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

## PostgreSQL (VM2)
Postgre is running in own container on VM2

## React + Next.js app (VM2)
Nodejs with Nextjs `control` running in container on VM2, its job is to process admin requests (react + next.js), keep track of replay files, periodically bundle replay files into CDN. `control` does not handle significant traffic and serves as control plane for administrator users. `control` writes to Redis when item/hero snapshot changes.

## OpenTelemetry + lgtm stack (VM2)
otel-lgtm container stream telemetry data, grafana is accessible via gcloud CLI.