"""Agent strategy iteration dashboard endpoints."""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from web.api.services.agent_service import (
    create_agent_run,
    get_agent_run,
    list_agent_runs,
    regenerate_approval_template,
)

router = APIRouter()


class CreateAgentRunRequest(BaseModel):
    objective: str
    run_id: str | None = None
    use_llm: bool = False
    propose_actions: bool = True
    write_approval_template: bool = True
    append_memory: bool = False


@router.get("/runs")
def runs() -> list[dict]:
    return list_agent_runs()


@router.get("/runs/{run_id}")
def run_detail(run_id: str) -> dict:
    return get_agent_run(run_id)


@router.post("/runs")
def create_run(request: CreateAgentRunRequest) -> dict:
    return create_agent_run(
        objective=request.objective,
        run_id=request.run_id,
        use_llm=request.use_llm,
        propose_actions=request.propose_actions,
        write_approval_template=request.write_approval_template,
        append_memory=request.append_memory,
    )


@router.post("/runs/{run_id}/approval-template")
def approval_template(run_id: str) -> dict:
    return regenerate_approval_template(run_id)
