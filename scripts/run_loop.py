#!/usr/bin/env python3
"""
Long-running refresher for GitHub Actions.

GitHub's `schedule:` cron is best-effort and on this repo routinely fires
8-9 hours late, so it cannot drive intraday refreshes. Instead one job stays
alive for up to ~5h45m, scrapes on a fixed Beijing-time timetable during A-share
trading hours, commits + pushes each refresh, and then re-dispatches the
workflow so the chain never breaks. Cron stays in the workflow only as a
resurrector in case the chain dies.

Timetable (Beijing time, Mon-Fri):
  09:30 .. 11:30 every 10 min, 13:00 .. 14:50 every 10 min, 15:05 final.
Ten minutes is also the practical floor: GitHub Pages soft-limits ~10 builds
per hour, and every data commit triggers one Pages build.
"""
import subprocess, sys, time
from datetime import datetime, timedelta, timezone

sys.stdout.reconfigure(encoding="utf-8")
BJ = timezone(timedelta(hours=8))
STEP_MIN = 10
MAX_RUNTIME = timedelta(hours=5, minutes=45)


def slots_for(day):
    """All refresh slots (aware datetimes) on a given date."""
    if day.weekday() >= 5:
        return []
    out = []
    def rng(h0, m0, h1, m1):
        t = datetime(day.year, day.month, day.day, h0, m0, tzinfo=BJ)
        end = datetime(day.year, day.month, day.day, h1, m1, tzinfo=BJ)
        while t <= end:
            out.append(t)
            t += timedelta(minutes=STEP_MIN)
    rng(9, 30, 11, 30)
    rng(13, 0, 14, 50)
    out.append(datetime(day.year, day.month, day.day, 15, 5, tzinfo=BJ))
    return out


def next_slot(now, last_done=None):
    d = now.date()
    for i in range(8):
        for s in slots_for(d + timedelta(days=i)):
            if last_done is not None and s <= last_done:
                continue
            # allow running a slot up to 4 min late (job start jitter)
            if s + timedelta(minutes=4) >= now:
                return s
    return None


def sh(*args, check=True):
    print("$", " ".join(args), flush=True)
    return subprocess.run(args, check=check)


def refresh_and_push():
    r = subprocess.run([sys.executable, "scripts/update_options.py"])
    if r.returncode != 0:
        print("scraper failed, skip commit", flush=True)
        return
    sh("git", "add", "data/")
    if subprocess.run(["git", "diff", "--quiet", "--staged"]).returncode == 0:
        print("No changes.", flush=True)
        return
    stamp = datetime.now(BJ).strftime("%Y-%m-%dT%H:%M%z")
    sh("git", "commit", "-q", "-m", f"data: refresh options ({stamp})")
    for attempt in range(4):
        if subprocess.run(["git", "pull", "-q", "--rebase", "-X", "theirs"]).returncode != 0:
            subprocess.run(["git", "rebase", "--abort"])
        if subprocess.run(["git", "push", "-q"]).returncode == 0:
            return
        time.sleep(5 * (attempt + 1))
    print("push failed after retries", flush=True)


def main():
    started = datetime.now(BJ)
    deadline = started + MAX_RUNTIME
    print(f"loop start {started:%Y-%m-%d %H:%M} BJ, deadline {deadline:%H:%M}", flush=True)
    last_done = None
    while True:
        now = datetime.now(BJ)
        slot = next_slot(now, last_done)
        if slot is None or slot > deadline:
            wait = (deadline - now).total_seconds()
            print(f"next slot {slot} beyond deadline; idle {wait/60:.0f} min then hand off", flush=True)
            if wait > 0:
                time.sleep(wait)
            break
        wait = (slot - now).total_seconds()
        if wait > 0:
            print(f"sleep {wait/60:.1f} min until {slot:%m-%d %H:%M}", flush=True)
            time.sleep(wait)
        refresh_and_push()
        last_done = slot
    print("loop end", flush=True)


if __name__ == "__main__":
    main()
