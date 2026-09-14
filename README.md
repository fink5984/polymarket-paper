# Polymarket BTC 5m Paper Lab

Research-only recorder and paper simulator. It cannot sign or submit orders and contains no wallet code.

## Railway

1. Create a Railway project from this directory/repository.
2. Add a persistent volume mounted at `/data`.
3. Set `DATA_DIR=/data` and `PAPER_ONLY=true`.
4. Deploy. Railway supplies `PORT`; `/health` is the health check and `/` is the dashboard.

Optional variables: `SAMPLE_SECONDS=2`, `EDGE_MIN=0.08`, `PAPER_STAKE=10`, `SLIPPAGE=0.01`.

## Local

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --port 8000
```

This first model is deliberately simple. Results are research observations, not evidence of future profitability.
