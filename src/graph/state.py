from __future__ import annotations

import operator
from typing import Annotated, List

from langchain_core.messages import BaseMessage
from typing_extensions import TypedDict


class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]
    task: str
    next_agent: str
    results: Annotated[dict, lambda a, b: {**a, **b}]
    iteration: int
    inventory_data: List[dict]
    vision_analysis: str
    risk_score: int
