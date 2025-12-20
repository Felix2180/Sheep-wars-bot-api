import os
import subprocess
import sys
import argparse
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.absolute()
TRACKED_FILE = str(SCRIPT_DIR / "tracked_users.txt")


def load_tracked_users() -> list[str]:
    """Load tracked usernames from tracked_users.txt."""
    if not os.path.exists(TRACKED_FILE):
        return []
    with open(TRACKED_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
    return lines


def run_api_get(username: str, api_key: str, snapshot_flags: list[str]) -> bool:
    """Run api_get.py for a user with given snapshot flags.
    
    Returns True if successful, False otherwise.
    """
    try:
        cmd = [sys.executable, "api_get.py", "-ign", username]
        if api_key:
            cmd.extend(["-key", api_key])
        cmd.extend(snapshot_flags)
        
        result = subprocess.run(cmd, cwd=str(SCRIPT_DIR), capture_output=True, text=True, timeout=30)
        return result.returncode == 0
    except Exception as e:
        print(f"[ERROR] Failed to run api_get.py for {username}: {e}")
        return False


def batch_update(schedule: str, api_key: str | None = None) -> dict:
    """Update all tracked users with appropriate snapshots.
    
    Args:
        schedule: One of 'daily', 'weekly', 'monthly', or 'all'
        api_key: Optional Hypixel API key; falls back to env var or hardcoded default
    
    Returns:
        Dict with results: {username: (success, snapshots_taken)}
    """
    users = load_tracked_users()
    if not users:
        print("[INFO] No tracked users found")
        return {}
    
    if api_key is None:
        api_key = os.environ.get("HYPIXEL_API_KEY") or "0adb2317-d343-4275-aa22-e7a980eb59df"
    
    results = {}
    
    # Map schedule to snapshot types
    schedule_map = {
        'daily': ['daily'],
        'weekly': ['yesterday'],
        'monthly': ['monthly'],
        'all': ['daily', 'yesterday', 'monthly']
    }
    
    print(f"[INFO] Processing {len(users)} tracked users with schedule '{schedule}'...")
    for username in users:
        snapshots_to_take = schedule_map.get(schedule, [])
        
        if not snapshots_to_take:
            print(f"[SKIP] {username} - invalid schedule")
            results[username] = (True, [])
            continue
        
        print(f"[RUN] {username} - taking snapshots: {', '.join(snapshots_to_take)}")
        flags = [f"-{s}" for s in snapshots_to_take]
        
        success = run_api_get(username, api_key, flags)
        
        if success:
            print(f"[OK] {username} - success")
            results[username] = (True, snapshots_to_take)
        else:
            print(f"[ERROR] {username} - failed")
            results[username] = (False, snapshots_to_take)
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Batch update tracked users with API snapshots")
    parser.add_argument("-schedule", choices=["daily", "weekly", "monthly", "all"], default="all",
                        help="Which snapshots to take")
    parser.add_argument("-key", "--api-key", help="Hypixel API key (optional, uses env or default)")
    args = parser.parse_args()
    
    results = batch_update(args.schedule, args.api_key)
    
    # Print summary
    successful = sum(1 for success, _ in results.values() if success)
    print(f"\n[SUMMARY] {successful}/{len(results)} users updated successfully")
    
    for username, (success, snapshots) in results.items():
        status = "[OK]" if success else "[ERROR]"
        print(f"  {status} {username}: {', '.join(snapshots) if snapshots else 'no snapshots'}")


if __name__ == "__main__":
    main()
