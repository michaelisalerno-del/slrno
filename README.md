# slrno

Server-deployable trading bot MVP for FMP market data, IG demo account connectivity, paper execution, configurable markets, and backtesting research.

## Status

This is an early scaffold. V1 is deliberately demo/paper only: it can validate provider credentials, model provider boundaries, run risk checks, and backtest strategies, but it does not expose a live IG order path.

## Project Layout

- `backend/`: FastAPI app, provider adapters, encrypted server-side settings, risk checks, market registry, and backtesting core.
- `frontend/`: React dashboard for settings, provider status, market wiring, and research summaries.

## Market Plugins

Markets are created through small provider-neutral plugin definitions before they are traded or backtested. The first built-in plugins are:

- `NAS100`: Nasdaq 100, searched on IG as `US Tech 100`, with FMP symbol candidate `^NDX`.
- `US500`: S&P 500, searched on IG as `US 500`, with FMP symbol candidate `^GSPC`.
- `XAUUSD`: Spot Gold, searched on IG as `Spot Gold`, with FMP symbol candidate `XAUUSD`.

The app intentionally leaves `ig_epic` blank until your IG demo credentials can search and confirm the exact account-specific instrument. That avoids hardcoding the wrong IG market early.

## Research Lab

The first research layer is modular and FMP-first. It records every strategy trial, including rejected ones, and only promotes passing ideas to a research-only watchlist.

- Classification metrics: ROC-AUC, PR-AUC, Brier score, log loss, positive rate, and top-quantile precision.
- Labels: forward-return labels and triple-barrier labels.
- Stackable probability modules: momentum continuation, mean-reversion stretch, and breakout continuation.
- Validation: rolling walk-forward folds with a final untouched holdout area.
- Gates: AUC quality, PR-AUC lift, top-bucket precision lift, Sharpe, profit, drawdown, fold stability, and trade count.

Passing candidates are not execution-ready until a later IG-price validation step confirms the same edge on the tradable IG EPIC.

## Server Deployment

Copy this repository to your server, create a `.env` from `.env.example`, then run:

```bash
docker compose up -d --build
```

Runtime data is stored in the Docker volume mounted at `/data` inside the backend container. Credentials entered in the settings page are encrypted before being written to that server-side volume. They are never committed to git.

Set `SLRNO_ALLOWED_ORIGINS` to the final dashboard origin, for example `https://bot.example.com`.

## Backend Development

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn app.main:app --reload
```

Set `SLRNO_HOME` to choose where runtime data is written during development. The server deployment sets it to `/data`.

## Frontend Development

```bash
cd frontend
npm install
npm run dev
```

Set `VITE_API_BASE_URL` if the backend is not running on `http://127.0.0.1:8000`.

## Tests

```bash
cd backend
pytest
```
