from fastapi import FastAPI, UploadFile, File
from typing import List
import tempfile, subprocess, json, os, sys
from pathlib import Path
from fastapi.responses import JSONResponse, RedirectResponse

app = FastAPI(title="Claim Resubmission API")

@app.get("/")
def root():
    return RedirectResponse(url="/docs")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ingest")
async def ingest(files: List[UploadFile] = File(...)):
    base_dir = Path(__file__).resolve().parent
    pipeline_path = str((base_dir / "pipeline.py").resolve())

    with tempfile.TemporaryDirectory() as td:
        saved_paths = []
        for f in files:
            dest = os.path.join(td, f.filename)
            with open(dest, "wb") as w:
                w.write(await f.read())
            saved_paths.append(dest)

        api_out_dir = os.path.join(td, "output", "api")
        os.makedirs(api_out_dir, exist_ok=True)
        out_json = os.path.join(api_out_dir, "resubmission.json")
        rej_json = os.path.join(api_out_dir, "rejections_log.json")

        cp = subprocess.run(
            [sys.executable, pipeline_path, *saved_paths, "--out", out_json, "--rejects", rej_json],
            capture_output=True, text=True, cwd=base_dir
        )

        # Response payload
        payload = {
            "returncode": cp.returncode,
            "stdout": cp.stdout,
            "stderr": cp.stderr,
            "candidates": [],
            "rejects_count": 0
        }

        if os.path.exists(out_json):
            with open(out_json, "r", encoding="utf-8") as f:
                payload["candidates"] = json.load(f)

        if os.path.exists(rej_json):
            with open(rej_json, "r", encoding="utf-8") as f:
                payload["rejects_count"] = len(json.load(f))

        # Error logging
        if cp.returncode != 0:
            return JSONResponse(status_code=500, content=payload)
        return payload
