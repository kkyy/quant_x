# quant_ex Web Frontend

React 19 + Vite + TypeScript + Tailwind CSS frontend for the local quant_ex dashboard.

## Pages

- Dashboard: system overview and runtime status
- Data Management: cache status, external data fetches, stock lookup
- Models: model training, model list, meta and feature importance
- Backtest: grid search, walk-forward validation, result browsing
- Signals: daily signal generation, rebalance simulation, notification tests
- Factors: factor registry, evaluation, factor mining
- Config: YAML config editor and strategy candidates
- Agent Runs: create/browse strategy-iteration agent runs, inspect plans/traces/commands/feedback, regenerate approval templates
- System: logs, tasks, runtime information

## API Pattern

Use `src/api/client.ts` for `get` / `post` / `put` / `del`. Long-running backend jobs should return a task id and stream status through `src/hooks/useSSE.ts`.

The Agent Runs page talks to `/api/agents`:

- `GET /api/agents/runs`
- `GET /api/agents/runs/{run_id}`
- `POST /api/agents/runs`
- `POST /api/agents/runs/{run_id}/approval-template`

The dashboard intentionally does not expose command execution for protected agent commands. Training, backtest, WFV, data fetch/update, notifications, and trading-like actions remain approval-gated in the CLI layer.

## Development

```bash
npm install
npm run dev
```

The Vite dev server proxies `/api` to the FastAPI backend on `:8000`.

Build:

```bash
npm run build
```

Production build output goes to `web/frontend/dist/` and is served by `web/run_web.py`.
