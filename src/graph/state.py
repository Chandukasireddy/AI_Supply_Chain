from __future__ import annotations

from typing import Annotated, Sequence
import operator

from langchain_core.messages import BaseMessage
from typing_extensions import TypedDict


class AgentState(TypedDict):
    # Accumulated conversation messages
    messages: Annotated[Sequence[BaseMessage], operator.add]
    # The user's original query
    task: str
    # Which agent the supervisor wants to call next ("FINISH" to end)
    next_agent: str
    # Keyed results from each agent run, merged each turn
    results: Annotated[dict, lambda a, b: {**a, **b}]
    # Number of supervisor iterations (guard against infinite loops)
    iteration: int
