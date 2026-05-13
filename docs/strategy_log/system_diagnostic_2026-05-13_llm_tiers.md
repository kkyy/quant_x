# System Diagnostic: 2026-05-13 LLM Tiers

## Layer Scores
| Layer | Score | Weakest Link | Highest Leverage Fix |
|---|---:|---|---|
| Data | 3 | No change. | Keep data actions approval-gated. |
| Factors | 3 | Role prompts can need different reasoning budgets. | Map factor/data roles to `quick` by default. |
| Model | 4 | Agent LLM model selection was split between role config and environment fallback. | Add explicit `llm.tiers` with model, reasoning effort, temperature, and max tokens. |
| Backtest | 4 | No change. | Keep feedback evaluator separate from LLM config. |
| Execution | 5 | Web-created agent runs could bypass YAML LLM tier config. | Load the same agent config from the Web service. |
| Web | 4 | Dashboard create-run path needed consistent LLM configuration. | Route Web-created runs through `config/agent_strategy_iteration.yaml`. |

## Key Findings
1. Agent roles already supported `model_tier`, but tier parameters were not first-class YAML configuration.
2. The LLM client now reads `llm.tiers.quick` and `llm.tiers.deep`, including `reasoning_effort`, `temperature`, and `max_tokens`.
3. The committed example uses direct `api_key` / `base_url` fields with an empty key placeholder; the real local config is gitignored and may contain private endpoint strings.

## Validation
- `./.venv/bin/python -m pytest test/test_agent_strategy_iteration.py test/test_web_dashboard.py`
- `./.venv/bin/python -m compileall agent/strategy_iteration web/api/services/agent_service.py run_agent_strategy_iteration.py`
- `./.venv/bin/python -c "from web.api.app import app; from agent.strategy_iteration.llm import OpenAICompatibleChatClient; print('OK')"`
