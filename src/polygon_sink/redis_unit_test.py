from __future__ import annotations

import os
import sys
import json
from typing import Optional

from dotenv import load_dotenv
import httpx
from redis import Redis as SyncRedis


def run_upstash_rest(url: str, token: str, key: str, value: str) -> bool:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    with httpx.Client(timeout=httpx.Timeout(10.0, read=15.0)) as client:
        set_body = ["SET", key, value]
        set_resp = client.post(url, headers=headers, json=set_body)
        if not set_resp.is_success:
            print(f"SET failed: {set_resp.status_code} {set_resp.text}")
            return False
        get_body = ["GET", key]
        get_resp = client.post(url, headers=headers, json=get_body)
        if not get_resp.is_success:
            print(f"GET failed: {get_resp.status_code} {get_resp.text}")
            return False
        try:
            data = get_resp.json()
        except Exception:
            print("GET response not JSON")
            return False
        result = data.get("result")
        ok = result == value
        if not ok:
            print(f"Value mismatch: expected={value} got={result}")
        return ok


def run_standard_redis(url: str, key: str, value: str) -> bool:
    client = SyncRedis.from_url(url, decode_responses=True)
    try:
        client.set(key, value)
        res = client.get(key)
        ok = res == value
        if not ok:
            print(f"Value mismatch: expected={value} got={res}")
        return ok
    finally:
        try:
            client.close()
        except Exception:
            pass


def main() -> None:
    load_dotenv()
    redis_url = os.getenv("REDIS_URL")
    redis_token = os.getenv("REDIS_TOKEN")
    if not redis_url:
        print("REDIS_URL is required for this test")
        sys.exit(1)

    key = "stock:test"
    value = "hello-world"

    if redis_url.lower().startswith("http"):
        if not redis_token:
            print("REDIS_TOKEN is required for Upstash REST test")
            sys.exit(1)
        ok = run_upstash_rest(redis_url, redis_token, key, value)
    else:
        ok = run_standard_redis(redis_url, key, value)

    if ok:
        print("redis_unit_test: PASS")
        sys.exit(0)
    else:
        print("redis_unit_test: FAIL")
        sys.exit(2)


if __name__ == "__main__":
    main()


