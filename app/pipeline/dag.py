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
            node: set(parents) for node, parents in predecessors.items()
        }
        self._validate()

    def _validate(self) -> None:
        known = set(self._predecessors)
        for node, predecessors in self._predecessors.items():
            dangling = predecessors - known
            if dangling:
                raise ValueError(f"{node} depends on unknown nodes: {dangling}")
        # Kahn's algorithm: if every node can be ordered, the graph is acyclic.
        indegree = {node: len(preds) for node, preds in self._predecessors.items()}
        ready = deque(node for node, degree in indegree.items() if degree == 0)
        ordered = 0
        while ready:
            node = ready.popleft()
            ordered += 1
            for successor in self.successors_of(node):
                indegree[successor] -= 1
                if indegree[successor] == 0:
                    ready.append(successor)
        if ordered != len(self._predecessors):
            raise ValueError("dependency graph has a cycle")

    @property
    def nodes(self) -> list[StepName]:
        """Every node, in declaration order."""
        return list(self._predecessors)

    @property
    def entry_nodes(self) -> list[StepName]:
        """Nodes with no predecessors (where the pipeline starts)."""
        return [
            node for node, predecessors in self._predecessors.items() if not predecessors
        ]

    @property
    def terminal_nodes(self) -> list[StepName]:
        """Nodes with no successors (where it ends)."""
        return [node for node in self._predecessors if not self.successors_of(node)]

    def predecessors_of(self, node: StepName) -> set[StepName]:
        return self._predecessors[node]

    def successors_of(self, node: StepName) -> list[StepName]:
        return [
            candidate
            for candidate, predecessors in self._predecessors.items()
            if node in predecessors
        ]

    def ready_successors(self, done: set[StepName]) -> list[StepName]:
        """Nodes not yet done whose predecessors are all done."""
        return [
            node for node, predecessors in self._predecessors.items()
            if node not in done and predecessors <= done
        ]


PREDECESSORS: dict[StepName, set[StepName]] = {
    StepName.OCR: set(),
    StepName.METADATA: {StepName.OCR},
    StepName.CHUNKING: {StepName.OCR},
    StepName.EXTERNAL_CALL: {StepName.METADATA, StepName.CHUNKING},
}

PIPELINE = DependencyGraph(PREDECESSORS)
