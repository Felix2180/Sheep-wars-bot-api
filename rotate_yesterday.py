"""
Helper script to copy daily snapshot to yesterday snapshot before daily refresh.
This preserves yesterday's stats before overwriting today's daily snapshot.
"""
import os
from pathlib import Path
from openpyxl import load_workbook

SCRIPT_DIR = Path(__file__).parent.absolute()
EXCEL_FILE = str(SCRIPT_DIR / "stats.xlsx")
TRACKED_FILE = str(SCRIPT_DIR / "tracked_users.txt")


def load_tracked_users() -> list[str]:
    """Load tracked usernames from tracked_users.txt."""
    if not os.path.exists(TRACKED_FILE):
        return []
    with open(TRACKED_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
    return lines


def rotate_daily_to_yesterday():
    """Copy daily snapshot (column F) to yesterday snapshot (column H) for all users."""
    if not os.path.exists(EXCEL_FILE):
        print("[SKIP] Excel file not found")
        return
    
    wb = load_workbook(EXCEL_FILE)
    users = load_tracked_users()
    updated_count = 0
    
    for username in users:
        if username not in wb.sheetnames:
            print(f"[SKIP] {username} - sheet not found")
            continue
        
        ws = wb[username]
        
        # Check if sheet has the expected layout (column F = Daily Snapshot, H = Yesterday Snapshot)
        header_f = ws.cell(row=1, column=6).value
        header_h = ws.cell(row=1, column=8).value
        
        if header_f != "Daily Snapshot" or header_h != "Yesterday Snapshot":
            print(f"[SKIP] {username} - unexpected layout")
            continue
        
        # Copy all values from column F (Daily Snapshot) to column H (Yesterday Snapshot)
        row = 2
        copied_rows = 0
        while True:
            daily_val = ws.cell(row=row, column=6).value
            if daily_val is None and row > 100:  # Stop if we've gone past data
                break
            
            # Copy daily snapshot to yesterday snapshot
            ws.cell(row=row, column=8, value=daily_val)
            if daily_val is not None:
                copied_rows += 1
            row += 1
        
        print(f"[OK] {username} - copied {copied_rows} rows from daily to yesterday")
        updated_count += 1
    
    wb.save(EXCEL_FILE)
    print(f"\n[SUMMARY] Rotated daily→yesterday for {updated_count}/{len(users)} users")
    return updated_count


if __name__ == "__main__":
    rotate_daily_to_yesterday()
