"""The pipeline dependency graph.

    ocr ──► metadata ──┐
        └─► chunking ──┴──► external_call

`DependencyGraph` holds the topology and answers what the transitioner needs:
predecessors/successors, entry/terminal nodes, and which steps become ready once a
set of steps is done. Adding a step is a data change to `PREDECESSORS`; the graph
is validated (acyclic, no dangling edges) when it's built.
"""
from __future__ import annotations

from collections import deque

from app.models import StepName


class DependencyGraph:
    def __init__(self, predecessors: dict[StepName, set[StepName]]) -> None:
        # Copy so the graph owns its data, declaration order is preserved.
        self._predecessors: dict[StepName, set[StepName]] = {
            node: set(preds) for node, preds in predecessors.items()
        }
        self._validate()

    def _validate(self) -> None:
        known = set(self._predecessors)
        for node, preds in self._predecessors.items():
            dangling = preds - known
            if dangling:
                raise ValueError(f"{node} depends on unknown nodes: {dangling}")
        # Kahn's algorithm: if every node can be ordered, the graph is acyclic.
        indegree = {n: len(p) for n, p in self._predecessors.items()}
        ready = deque(n for n, d in indegree.items() if d == 0)
        ordered = 0
        while ready:
            node = ready.popleft()
            ordered += 1
            for succ in self.successors_of(node):
                indegree[succ] -= 1
                if indegree[succ] == 0:
                    ready.append(succ)
        if ordered != len(self._predecessors):
            raise ValueError("dependency graph has a cycle")

    @property
    def nodes(self) -> list[StepName]:
        """Every node, in declaration order."""
        return list(self._predecessors)

    @property
    def entry_nodes(self) -> list[StepName]:
        """Nodes with no predecessors (where the pipeline starts)."""
        return [n for n, preds in self._predecessors.items() if not preds]

    @property
    def terminal_nodes(self) -> list[StepName]:
        """Nodes with no successors (where it ends)."""
        return [n for n in self._predecessors if not self.successors_of(n)]

    def predecessors_of(self, node: StepName) -> set[StepName]:
        return self._predecessors[node]

    def successors_of(self, node: StepName) -> list[StepName]:
        return [n for n, preds in self._predecessors.items() if node in preds]

    def ready_successors(self, done: set[StepName]) -> list[StepName]:
        """Nodes not yet done whose predecessors are all done."""
        return [
            n for n, preds in self._predecessors.items()
            if n not in done and preds <= done
        ]


PREDECESSORS: dict[StepName, set[StepName]] = {
    StepName.OCR: set(),
    StepName.METADATA: {StepName.OCR},
    StepName.CHUNKING: {StepName.OCR},
    StepName.EXTERNAL_CALL: {StepName.METADATA, StepName.CHUNKING},
}

PIPELINE = DependencyGraph(PREDECESSORS)
