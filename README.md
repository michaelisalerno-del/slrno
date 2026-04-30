# slrno

Server-deployable trading bot MVP for EODHD market data, IG demo account connectivity, paper execution, configurable markets, and backtesting research.

## Status

This is an early scaffold. V1 is deliberately demo/paper only: it can validate provider credentials, model provider boundaries, run risk checks, and backtest strategies, but it does not expose a live IG order path.

## Project Layout

- `backend/`: FastAPI app, provider adapters, encrypted server-side settings, risk checks, market registry, and backtesting core.
- `frontend/`: React dashboard for settings, provider status, market wiring, and research summaries.

## Market Plugins

Markets are created through small provider-neutral plugin definitions before they are traded or backtested. The built-in research universe now covers core IG spread-betting categories with direct EODHD symbols:

- Indices: US Tech 100, US 500, Wall Street, FTSE 100, Germany 40, France 40, EU Stocks 50, Japan 225, Hong Kong HS50, Australia 200, and volatility markets.
- Forex: major USD pairs plus liquid EUR/JPY and GBP/JPY crosses.
- Commodities: spot gold/silver via EODHD forex metals plus EODHD commodities for Brent, WTI, natural gas, and copper.
- Rates, sectors/shares, options, knock-outs, bungees, binaries, and sprints are surfaced through the IG spread-bet engine registry; product types without a linear P/L model are marked as needing a product-specific model.

The app intentionally leaves `ig_epic` blank until your IG demo credentials can search and confirm the exact account-specific instrument. That avoids hardcoding the wrong IG market early.

## Research Lab

The first research layer is modular and EODHD-first. It records every strategy trial, including rejected ones, and only promotes passing ideas to a research-only watchlist.

- Classification metrics: ROC-AUC, PR-AUC, Brier score, log loss, positive rate, and top-quantile precision.
- Labels: forward-return labels and triple-barrier labels.
- Stackable probability modules: momentum continuation, mean-reversion stretch, and breakout continuation.
- Validation: rolling walk-forward folds with a final untouched holdout area.
- Gates: AUC quality, PR-AUC lift, top-bucket precision lift, Sharpe, profit, drawdown, fold stability, and trade count.
- Critic: a modular research critic scores each run for audit quality, EODHD-only evidence, overfitting smells, one-fold dependency, weak AUC/PR-AUC lift, low trade count, and fragile economics.

Passing candidates are not execution-ready until a later IG-price validation step confirms the same edge on the tradable IG EPIC.

## Automated Edge Discovery

The backend includes a one-command, profit-first edge discovery pipeline. It ingests cached EODHD bars, searches the adaptive IG-aware strategy families, applies realistic and stressed cost assumptions, then writes timestamped artifacts with a KEEP/REJECT leaderboard, secondary Sharpe tracking, an aggregation report, and a 30-day paper-trading protocol.

From the server:

```bash
cd /opt/slrno
docker compose exec backend python -m app.edge_discovery --config configs/edge_discovery.yaml --mode quick
docker compose exec backend python -m app.edge_discovery --config configs/edge_discovery.yaml --mode deep
```

Save the EODHD token in the dashboard settings or set `EODHD_API_TOKEN` in the backend environment for CLI-only runs.

Artifacts are written under `/data/artifacts/edge_discovery` inside the backend container volume by default. A candidate is KEEP only if holdout net profit, stressed-cost profit, fold consistency, trade count, drawdown, cost/gross efficiency, and profit-concentration gates all pass. Sharpe >= 2 is tracked as an aspirational quality target, not a reason to keep a fragile candidate.

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
