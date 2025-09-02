import subprocess
import time

scripts = [
    "buildDailyData.py",
    "overviewData.py",
    "typeData.py"
]

while True:
    for script in scripts:
        print(f"Running {script}...")
        result = subprocess.run(
            ["python", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        print(result.stdout)
        print(result.stderr)
        time.sleep(2)
    print("Cycle complete. Waiting 24h...")
    time.sleep(24 * 60 * 50)
