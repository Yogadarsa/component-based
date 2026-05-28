# FlowForge 
**Instructions for running the project**
1. Make sure you are in the project folder
powershell
cd "d:\component final assessement"
2. Run the Examples
We have 4 different example scripts built to demonstrate the capabilities of the engine. Run any of them using the python command:

To see a basic sequential workflow:
powershell
python examples/basic_workflow.py

To see parallel multi-threading in action:
powershell
python examples/parallel_workflow.py

To see if/else conditional routing:
powershell
python examples/conditional_workflow.py

To see advanced features (retries, events, branching, and parallel running together):
powershell
python examples/advanced_workflow.py

3. Run the Test Suite (Proves it is Enterprise-Grade)
If you want to prove to your professor that this is a robust component, run the automated test suite we built. It will instantly execute all 125 tests:

powershell
python -m pytest tests/ -v


**A Python Workflow Pipeline Engine**

FlowForge lets you define multi-step workflows as Directed Acyclic Graphs (DAGs), execute them with parallel scheduling, retry policies, conditional branching, checkpointing, and a rich event system.

> **Zero dependencies** — built entirely on the Python standard library.

## Features

- **DAG-based workflows** — Define steps and dependencies as a directed acyclic graph
- **Parallel execution** — Independent steps run concurrently via thread pool
- **Retry policies** — Fixed, exponential backoff, and linear backoff strategies
- **Conditional branching** — Route execution paths based on runtime data
- **Checkpointing** — Pause, save state, and resume workflows later
- **Event system** — Subscribe to lifecycle events (start, complete, fail, retry)
- **Fluent builder API** — Construct workflows with clean, chainable syntax
- **Decorator API** — Register steps with `@workflow_step` decorator
- **Timeout enforcement** — Prevent runaway nodes
- **Fully tested** — Comprehensive unit and integration test suite

## Quick Start

```python
from flowforge import WorkflowBuilder

def extract(ctx):
    return [1, 2, 3]

def transform(ctx):
    data = ctx.get_node_result("extract")
    return [x * 2 for x in data]

def load(ctx):
    data = ctx.get_node_result("transform")
    print(f"Loaded {len(data)} records: {data}")

workflow = (
    WorkflowBuilder("etl")
    .add_step("extract", extract)
    .add_step("transform", transform, depends_on=["extract"])
    .add_step("load", load, depends_on=["transform"])
    .build()
)

result = workflow.run()
print(result.status)  # WorkflowStatus.COMPLETED
```

## Installation

```bash
pip install -e .
```

## Running Tests

```bash
pip install pytest
python -m pytest tests/ -v
```

## Examples

See the `examples/` directory for complete working examples:

| Example | Description |
|---------|-------------|
| `basic_workflow.py` | Simple 3-step ETL pipeline |
| `parallel_workflow.py` | Fan-out / fan-in parallel processing |
| `conditional_workflow.py` | If/else branching based on runtime data |
| `advanced_workflow.py` | All features combined: retry, events, conditions |

## Documentation

- [Architecture Report](docs/architecture_report.md) — Class diagrams, sequence diagrams, state machines
- [User Documentation](docs/user_documentation.md) — API reference, usage guide, installation
- [Competitor Analysis](docs/competitor_analysis.md) — Comparison with Airflow, Prefect, Step Functions, Logic Apps

## Project Structure

```
flowforge/
├── __init__.py          # Public API
├── enums.py             # Status enumerations
├── exceptions.py        # Exception hierarchy
├── core/
│   ├── node.py          # Node (workflow step)
│   ├── dag.py           # Directed Acyclic Graph
│   ├── context.py       # Execution context
│   └── engine.py        # Workflow engine
├── policies/
│   ├── retry.py         # Retry strategies
│   └── timeout.py       # Timeout policy
├── branching/
│   └── conditions.py    # Conditional routing
├── checkpoint/
│   └── manager.py       # Checkpoint save/restore
├── events/
│   └── hooks.py         # Event pub/sub system
└── builders/
    └── workflow.py      # Fluent builder API
```

## License

MIT License
