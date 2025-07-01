# application/pipeline.py
from __future__ import annotations

import logging
from typing import Iterable, List, Dict, Callable, Any

logger = logging.getLogger("application.pipeline")


# ────────────────────────────────────────────────────────────────────────────
class Step:
    """
    Pequeño wrapper para un paso de pipeline.

    • func: Callable que recibe / devuelve un dict-contexto.
    • name: etiqueta legible para logging / métricas.
    """

    def __init__(self, func: Callable[[Dict[str, Any]], Dict[str, Any] | None], name: str):
        self.func = func
        self.name = name

    # --------------------------------------------------
    def run(self, ctx: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("▶ %s …", self.name)
        result = self.func(ctx) or {}
        logger.info("✓ %s", self.name)
        # Mezclamos resultados en el mismo contexto
        ctx.update(result)
        return ctx

    # Representación bonita (útil en debug / tests)
    # --------------------------------------------------
    def __repr__(self) -> str:  # pragma: no cover
        return f"<Step {self.name}>"


# ────────────────────────────────────────────────────────────────────────────
class Pipeline:
    """
    Ejecuta una serie de Step secuenciales con un único contexto (dict).

    Puede instanciarse de dos formas:
        Pipeline(step1, step2, step3)
        Pipeline([step1, step2, step3])
    """

    def __init__(self, *steps: Step | Iterable[Step]):
        # Permitir tanto *steps como Pipeline([steps…])
        if len(steps) == 1 and isinstance(steps[0], (list, tuple)):
            self.steps: List[Step] = list(steps[0])
        else:
            self.steps = list(steps)

        # Validación rápida
        for s in self.steps:
            if not isinstance(s, Step):
                raise TypeError("Todos los elementos deben ser instancia de Step")

    # --------------------------------------------------
    def __call__(self, ctx: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if ctx is None:
            ctx = {}

        for step in self.steps:
            ctx = step.run(ctx)

        return ctx

    # Representación bonita
    # --------------------------------------------------
    def __repr__(self) -> str:  # pragma: no cover
        names = ", ".join(s.name for s in self.steps)
        return f"<Pipeline [{names}]>"
