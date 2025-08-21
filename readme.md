### Run locally

ppython -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python pipeline.py data/sample.csv data/sample.json

### Start API

uvicorn api:app --reload --port 8000
