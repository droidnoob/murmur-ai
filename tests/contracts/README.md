# tests/contracts/

**Shared contract test suites — one class per Protocol.** Every concrete implementation of a Protocol runs through the *same* suite. This is how the project enforces the Interfaces Rule from `CLAUDE.md`: a Protocol is a behavioural contract, and every implementation must satisfy it identically.

## Why this exists

Murmur's pluggable surface (Backend, ContextPasser, Registry, …) is keyed on `typing.Protocol`. Without a shared suite, behaviour can drift silently between implementations — `ThreadBackend.spawn` works one way, `JobBackend.spawn` works another, and tests never catch the divergence because each backend is tested in isolation.

The shared suite fixes that: write the test once, parametrize the implementation.

## Pattern

Each contract is a base class that depends on a single fixture (the implementation under test):

```python
# tests/contracts/backend_contract.py

class BackendContract:
    @pytest.fixture
    def backend(self) -> Backend:
        raise NotImplementedError(
            "subclass must override `backend` fixture with a concrete instance"
        )

    async def test_spawn_returns_handle_for_agent_and_task(
        self, backend: Backend, agent, task, context,
    ) -> None:
        handle = await backend.spawn(agent, task, context)
        assert handle.agent_name == agent.name
        ...
```

Each implementation provides the fixture in its own test module:

```python
# tests/backends/test_thread.py
from tests.contracts.backend_contract import BackendContract

class TestThreadBackend(BackendContract):
    @pytest.fixture
    def backend(self) -> Backend:
        return ThreadBackend()

    # add ThreadBackend-specific tests below if needed
```

Pytest runs every method on `BackendContract` for every subclass that overrides `backend`. New tests added to the contract automatically apply to every implementation.

## Current contracts

| File                              | Protocol                                    |
| --------------------------------- | ------------------------------------------- |
| `backend_contract.py`             | `core.protocols.backend.Backend`            |
| `context_passer_contract.py`      | `core.protocols.context.ContextPasser`      |
| `registry_contract.py`            | `core.protocols.registry.Registry`          |

More land alongside their Protocols (Worker, ToolProvider, ToolExecutor, EventEmitter, Router, Pipeline, Stage, Middleware) as those gain real implementations.

## Rules

- **Protocol-keyed only.** A contract test must depend only on the Protocol surface — never on internals of a specific implementation.
- **No skips for "stub" implementations.** If a method isn't ready, the contract should fail; that surfaces the gap. Use `@pytest.mark.xfail(strict=True)` per-method only if the gap is intentional and tracked.
- **One file per Protocol.** Don't bundle. Future-you will thank you.
- **Run via `pytest -k <Name>Contract`** to verify a single contract across all implementations.

## Adding a new pluggable

1. Define the Protocol in `src/murmur/core/protocols/<name>.py`.
2. Write the contract suite here (`tests/contracts/<name>_contract.py`).
3. Implement the first concrete in its sibling package.
4. Add the implementation's test module subclassing the contract.

If steps 1 and 2 are skipped, you have escaped the rule. Don't.
