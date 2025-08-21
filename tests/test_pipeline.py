import json, subprocess, sys

def test_runs():
    cp = subprocess.run(
        [sys.executable, "pipeline.py", "data/sample.csv", "data/sample.json", "--out", "tmp_out.json", "--rejects", "tmp_rej.json"],
        capture_output=True, text=True
    )
    assert cp.returncode == 0
    data = json.loads(open("tmp_out.json").read())
    assert isinstance(data, list)
