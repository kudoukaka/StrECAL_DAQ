#!/usr/bin/env bash

#   ./load_redis_params.sh <config.json>

set -euo pipefail

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_AUTH="${REDIS_AUTH:-}"

JSON_FILE="${1:-}"
if [[ -z "$JSON_FILE" ]]; then
    echo "Usage: $0 <config.json>" >&2
    exit 1
fi

if [[ ! -f "$JSON_FILE" ]]; then
    echo "Error: file not found: $JSON_FILE" >&2
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "Error: jq is required but not installed." >&2
    exit 1
fi

if ! command -v redis-cli >/dev/null 2>&1; then
    echo "Error: redis-cli is required but not installed." >&2
    exit 1
fi

RC=(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT")
if [[ -n "$REDIS_AUTH" ]]; then
    RC+=(-a "$REDIS_AUTH" --no-auth-warning)
fi

# Sanity-check the JSON before touching Redis
if ! jq empty "$JSON_FILE" >/dev/null 2>&1; then
    echo "Error: $JSON_FILE is not valid JSON." >&2
    exit 1
fi

CHAIN_NUMBERS=$(jq -r '.chains[].chain' "$JSON_FILE")

for CHAIN in $CHAIN_NUMBERS; do
    LIST_KEY="parameters:eros-chain:${CHAIN}"

    echo "== Chain ${CHAIN} =="
    echo "  Clearing ${LIST_KEY}"
    "${RC[@]}" DEL "$LIST_KEY" > /dev/null

    # Boards for this chain, sorted by their "board" index ascending
    BOARDS_JSON=$(jq -c --argjson chain "$CHAIN" \
        '.chains[] | select(.chain == $chain) | .boards | sort_by(.board) | .[]' \
        "$JSON_FILE")

    while IFS= read -r BOARD; do
        [[ -z "$BOARD" ]] && continue

        BOARD_NUM=$(jq -r '.board' <<< "$BOARD")
        BOARD_IP=$(jq -r '.board_ip' <<< "$BOARD")

        echo "  Board ${BOARD_NUM} -> ${BOARD_IP}"

        # 1) push IP onto the ordered list, in board-index order
        "${RC[@]}" RPUSH "$LIST_KEY" "$BOARD_IP" > /dev/null

        # 2) store the full board configuration as a hash
        HASH_KEY="parameters:eros:chain:${CHAIN}:board:${BOARD_NUM}"
        "${RC[@]}" DEL "$HASH_KEY" > /dev/null

        while IFS=$'\t' read -r FIELD VALUE; do
            [[ -z "$FIELD" ]] && continue
            "${RC[@]}" HSET "$HASH_KEY" "$FIELD" "$VALUE" > /dev/null
        done < <(jq -r 'to_entries[] | select(.key != "board") | "\(.key)\t\(.value)"' <<< "$BOARD")

    done <<< "$BOARDS_JSON"
done

echo "Done."