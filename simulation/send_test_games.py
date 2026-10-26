import json
import time
import urllib.request
import urllib.error

BASE_URL = "https://observelol.vbrokh.dev"
NUM_GAMES = 10
EVENT_DELAY = 0.1
GAME_DELAY = 0.1


def post_json(url: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        if resp.status != 200:
            raise RuntimeError(f"POST {url} failed: {resp.status}")


def get_json(url: str) -> dict:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req) as resp:
        if resp.status != 200:
            raise RuntimeError(f"GET {url} failed: {resp.status}")
        return json.loads(resp.read().decode("utf-8"))


def send_game(game_num: int, total_games: int) -> None:
    # 1. Acquire new game ID
    new_game = get_json(f"{BASE_URL}/api/new-game/")
    game_id = new_game["game_id"]
    print(f"[{game_num}/{total_games}] Initialized game {game_id}")
    time.sleep(EVENT_DELAY)

    # 2. Build minimum valid sequence required by worker validation
    events = []

    # Index 0: GameStarted
    events.append({
        "game_id": game_id,
        "timestamp_ms": 0,
        "event_index": 0,
        "event_type": 1,
        "payload": {"started_at": int(time.time())},
    })

    # Indices 1..10: HeroSelected (slots 0..9)
    for slot in range(10):
        events.append({
            "game_id": game_id,
            "timestamp_ms": 100 + slot * 10,
            "event_index": 1 + slot,
            "event_type": 2,
            "payload": {
                "slot": slot,
                "hero_id": 100 + slot,
                "version_id": 1,
            },
        })

    # Index 11: RoundStart
    events.append({
        "game_id": game_id,
        "timestamp_ms": 250,
        "event_index": 11,
        "event_type": 3,
        "payload": {},
    })

    # Index 12: GameEnds
    events.append({
        "game_id": game_id,
        "timestamp_ms": 300,
        "event_index": 12,
        "event_type": 7,
        "payload": {},
    })

    # 3. Stream events to ingest
    for ev in events:
        post_json(f"{BASE_URL}/api/event/", ev)
        time.sleep(EVENT_DELAY)

    print(f"[{game_num}/{total_games}] Completed sending 13 events for game {game_id}")


def main() -> None:
    print(f"Target host: {BASE_URL}")
    print(f"Starting sequential simulation of {NUM_GAMES} games...\n")

    for i in range(1, NUM_GAMES + 1):
        send_game(i, NUM_GAMES)
        time.sleep(GAME_DELAY)

    print("\nAll 10 games successfully dispatched!")


if __name__ == "__main__":
    main()
