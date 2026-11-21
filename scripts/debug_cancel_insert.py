"""Debug only: loop the INSERT cancellation case and dump native stacks on a hang."""
import os
import subprocess
import sys
import threading
import time

import duckdb

ext, cs = sys.argv[1], sys.argv[2]
ROUNDS = int(sys.argv[3]) if len(sys.argv) > 3 else 12
SQL = "INSERT INTO rw.main.RW_LOG SELECT -93, 'cancel' FROM range(100000000) t(i)"


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{ext}'")
    con.execute(f"ATTACH '{cs}' AS rw (TYPE firebird, READ_WRITE)")
    return con


def dump(tag):
    print(f"==== native stacks: {tag}", flush=True)
    subprocess.run(["sudo", "-n", "gdb", "-p", str(os.getpid()), "-batch", "-q",
                    "-ex", "set pagination off", "-ex", "thread apply all bt 40"],
                   stdout=sys.stdout, stderr=subprocess.STDOUT, timeout=120)
    print(f"==== end: {tag}", flush=True)


hangs = 0
slow = 0
bad = 0
for i in range(ROUNDS):
    con = connect()
    result = {}

    def run():
        t0 = time.monotonic()
        try:
            con.execute(SQL).fetchall()
            result["outcome"] = "finished"
        except duckdb.InterruptException:
            result["outcome"] = "interrupted"
        except Exception as e:  # noqa: BLE001
            result["outcome"] = f"{type(e).__name__}: {str(e).splitlines()[0][:300]}"
        result["secs"] = time.monotonic() - t0

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    delay = 1.0 + (i % 6) * 0.37
    time.sleep(delay)
    con.interrupt()
    worker.join(15.0)
    if worker.is_alive():
        hangs += 1
        print(f"round {i}: HANG (interrupt after {delay:.2f} s)", flush=True)
        dump(f"round {i}, +15 s")
        time.sleep(20)
        dump(f"round {i}, +35 s")
        print(f"round {i}: after 35 s alive={worker.is_alive()} {result}", flush=True)
        if hangs >= 2:
            break
        continue
    late = result["secs"] - delay
    con.close()
    check = connect()
    kept = check.execute("SELECT * FROM firebird_query('rw', 'SELECT COUNT(*) FROM RW_LOG WHERE ID = -93')").fetchall()[0][0]
    check.close()
    slow += late > 0.8
    bad += result["outcome"] != "interrupted" or kept != 0
    print(f"round {i}: {result['outcome']} after {result['secs']:.1f} s (interrupt after {delay:.2f} s, "
          f"{late:.2f} s late{', SLOW' if late > 0.8 else ''}), {kept} row(s) kept", flush=True)

print(f"hangs: {hangs} slow: {slow} bad: {bad}", flush=True)
os._exit(0)
