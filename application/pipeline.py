# application/pipeline.py
from __future__ import annotations
import logging
from typing import Callable, List, Any

logger = logging.getLogger(__name__)


class Step:
    """Simple wrapper para cada paso del pipeline."""
    def __init__(self, name: str, func: Callable[..., Any]):
        self.name, self.func = name, func

    def run(self, context: dict) -> dict:
        logger.info("▶ %s …", self.name)
        result = self.func(context) or {}
        context.update(result)
        logger.info("✓ %s", self.name)
        return context


class Pipeline:
    def __init__(self, steps: List[Step]):
        self.steps = steps

    def __call__(self, context: dict | None = None) -> dict:
        ctx = context or {}
        for step in self.steps:
            ctx = step.run(ctx)
        return ctx
