# System Diagnostic: 2026-05-13 Phase 5

## Layer Scores
| Layer | Score | Weakest Link | Highest Leverage Fix |
|---|---:|---|---|
| Data | 3 | Agent-created data refresh proposals remain approval-gated and file-based. | Surface approval templates in the dashboard without execution controls. |
| Factors | 3 | Factor experiment plans were hard to browse across run folders. | Add a dashboard page for agent plan, commands, feedback, and raw artifacts. |
| Model | 4 | Model/training actions must remain protected. | Expose command proposals and approval templates, but no execution endpoint. |
| Backtest | 4 | Feedback handoff paths were visible only in files. | Surface `execution_summary.md` and command artifacts in the UI. |
| Execution | 5 | Phase 4 artifacts were auditable but not easy to inspect. | Add read-mostly `/api/agents` endpoints and a dashboard browser. |
| Web | 4 | Dashboard had no agent research workflow surface. | Add Agent Runs navigation, create form, detail tabs, and approval-template regeneration. |

## Key Findings
1. Phase 5 brings the agent strategy iteration loop into the Web Dashboard without adding command execution from the UI.
2. The backend exposes safe browse/create/regenerate endpoints under `/api/agents`, with path traversal protection for run folders.
3. The frontend adds an Agent Runs page for run list, detail tabs, markdown artifacts, JSON snippets, create-run form, and approval template regeneration.

## Change Plan
| Change | Files | Validation |
|---|---|---|
| Add agent run artifact service. | `web/api/services/agent_service.py` | FastAPI tests for list/detail/create/regenerate and traversal guard. |
| Add `/api/agents` router. | `web/api/routers/agents.py`, `web/api/app.py`, `web/api/deps.py` | `from web.api.app import app` and TestClient checks. |
| Add Agent Runs frontend page. | `web/frontend/src/pages/AgentRunsPage.tsx` | `npm run build`. |
| Add navigation and i18n. | `web/frontend/src/App.tsx`, `Sidebar.tsx`, `en.json`, `zh.json`, `types.ts` | Frontend build and route chunk generation. |
| Record smoke run. | `docs/strategy_log/agent_runs/phase5_dashboard_integration_smoke/` | Created through `/api/agents/runs`. |

## Validation
- `./.venv/bin/python -m pytest test/test_web_dashboard.py test/test_agent_strategy_iteration.py test/test_grid_search.py test/test_walk_forward_validation.py`
- `./.venv/bin/python -c "from web.api.app import app; print('OK')"`
- `cd web/frontend && npm run build`
- TestClient `POST /api/agents/runs` and `GET /api/agents/runs/phase5_dashboard_integration_smoke`
