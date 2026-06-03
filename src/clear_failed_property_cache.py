"""One-time cleanup: purge poisoned (failed) property cache entries.

Background: a Socrata 403 (rate limit) used to be cached as a "No data found"
failure. The file cache has no TTL and failed rows were never evicted, so those
addresses kept returning the stale failure on every run. This removes only the
FAILED entries (success != True) from whichever cache backend is in use,
preserving good cached data so you don't have to re-scrape everything.

Usage (from the src/ directory):
    python clear_failed_property_cache.py            # dry run, reports only
    python clear_failed_property_cache.py --apply    # actually delete

Handles both the file cache (cache/ dir) and Redis (localhost:6379) if reachable.
"""

import os
import sys
import json
import glob

DRY_RUN = "--apply" not in sys.argv


def _is_failed(entry: dict) -> bool:
    """A cache entry is poisoned if its inner result is not a success."""
    try:
        result = entry.get("data", {}).get("data", {})
        return result.get("success") is not True
    except AttributeError:
        return False


def clean_file_cache(cache_dir: str = "cache") -> None:
    if not os.path.isdir(cache_dir):
        print(f"[file] No cache directory at '{cache_dir}', skipping.")
        return

    pattern = os.path.join(cache_dir, "*_property_*.json")
    files = glob.glob(pattern)
    failed = 0
    for path in files:
        try:
            with open(path, "r") as f:
                entry = json.load(f)
        except Exception as e:
            print(f"[file] Could not read {path}: {e}")
            continue

        if _is_failed(entry):
            failed += 1
            if DRY_RUN:
                print(f"[file] WOULD DELETE {os.path.basename(path)}")
            else:
                try:
                    os.remove(path)
                    print(f"[file] Deleted {os.path.basename(path)}")
                except Exception as e:
                    print(f"[file] Failed to delete {path}: {e}")

    verb = "would remove" if DRY_RUN else "removed"
    print(f"[file] Scanned {len(files)} property entries, {verb} {failed} failed.")


def clean_redis_cache() -> None:
    try:
        import redis
        client = redis.Redis(host="localhost", port=6379, db=0,
                             socket_connect_timeout=5)
        client.ping()
    except Exception as e:
        print(f"[redis] Not reachable ({e}), skipping.")
        return

    scanned = 0
    failed = 0
    for key in client.scan_iter(match="cache:*_property:*", count=500):
        scanned += 1
        try:
            raw = client.get(key)
            if not raw:
                continue
            entry = json.loads(raw)
        except Exception as e:
            print(f"[redis] Could not read {key}: {e}")
            continue

        if _is_failed(entry):
            failed += 1
            key_str = key.decode() if isinstance(key, bytes) else key
            if DRY_RUN:
                print(f"[redis] WOULD DELETE {key_str}")
            else:
                client.delete(key)
                print(f"[redis] Deleted {key_str}")

    verb = "would remove" if DRY_RUN else "removed"
    print(f"[redis] Scanned {scanned} property entries, {verb} {failed} failed.")


if __name__ == "__main__":
    mode = "DRY RUN (no deletions). Re-run with --apply to delete." if DRY_RUN else "APPLYING deletions."
    print(f"=== Clear failed property cache: {mode} ===")
    clean_file_cache()
    clean_redis_cache()
    print("=== Done ===")
