# FlowForge — Architecture Report

## 1. Executive Summary

FlowForge is a lightweight, embeddable Python library that enables developers to define, execute, and manage multi-step workflows as Directed Acyclic Graphs (DAGs). The engine provides parallel execution, configurable retry policies, conditional branching, in-memory checkpointing, and a publish/subscribe event system — all with **zero external dependencies**.

This document provides the complete architectural blueprint of the component, including functional overview, technical and business restrictions, and comprehensive UML-style diagrams rendered in Mermaid.js.

---

## 2. Component Functions

### 2.1 Core Functions

| # | Function | Description |
|---|----------|-------------|
| F1 | **DAG Construction** | Define workflow steps as nodes and their dependencies as directed edges |
| F2 | **Topological Scheduling** | Automatically determine valid execution order using Kahn's algorithm |
| F3 | **Cycle Detection** | DFS-based cycle detection prevents invalid dependency graphs |
| F4 | **Parallel Execution** | Independent nodes execute concurrently via thread pool |
| F5 | **Sequential Execution** | Dependent nodes execute in topological order |
| F6 | **Data Passing** | Thread-safe execution context allows data flow between nodes |
| F7 | **Retry Policies** | Configurable retry strategies (fixed, exponential, linear backoff) |
| F8 | **Timeout Enforcement** | Maximum execution duration per node |
| F9 | **Conditional Branching** | Runtime conditions gate node execution (skip or proceed) |
| F10 | **Checkpointing** | Save and restore workflow state for pause/resume |
| F11 | **Event System** | Pub/sub hooks for lifecycle monitoring |
| F12 | **Fluent Builder API** | Chainable API for workflow construction |
| F13 | **Decorator API** | `@workflow_step` decorator for declarative step registration |
| F14 | **Workflow Validation** | Comprehensive pre-execution validation |

### 2.2 Function Dependency Matrix

```mermaid
graph LR
    F1[DAG Construction] --> F2[Topological Sort]
    F1 --> F3[Cycle Detection]
    F2 --> F4[Parallel Execution]
    F2 --> F5[Sequential Execution]
    F4 --> F6[Data Passing]
    F5 --> F6
    F4 --> F7[Retry Policies]
    F5 --> F7
    F7 --> F8[Timeout]
    F4 --> F9[Conditional Branching]
    F5 --> F9
    F4 --> F10[Checkpointing]
    F4 --> F11[Event System]
    F12[Builder API] --> F1
    F13[Decorator API] --> F12
    F14[Validation] --> F1
```

---

## 3. Technical Restrictions

| ID | Restriction | Impact | Mitigation |
|----|-------------|--------|------------|
| TR-1 | **Python ≥ 3.9 required** | Uses `typing` features and `datetime.fromisoformat()` | Documented in installation requirements |
| TR-2 | **Single-process execution** | Thread pool is bound to the host process; no cross-machine distribution | Architecture supports future pluggable executors |
| TR-3 | **GIL limitation** | Python's GIL means CPU-bound nodes won't achieve true parallelism | Use for I/O-bound tasks; document recommendation |
| TR-4 | **In-memory checkpoints** | Checkpoints are lost on process termination | Extensible `CheckpointManager` supports future persistent backends |
| TR-5 | **Synchronous node callables** | All node functions must be synchronous (not `async`) | Could be extended with `asyncio` executor |
| TR-6 | **Thread-safety of user code** | Node functions must be thread-safe when sharing mutable state | `ExecutionContext` provides thread-safe `set`/`get` |
| TR-7 | **No circular dependencies** | DAG structure inherently prohibits cycles | Validated at edge-addition time with DFS |

---

## 4. Business Restrictions

| ID | Restriction | Rationale |
|----|-------------|-----------|
| BR-1 | **No GUI / Dashboard** | Library component — visual tools are consumer-facing applications |
| BR-2 | **No authentication / authorisation** | Out of scope for an embeddable library |
| BR-3 | **No persistent storage** | Keeps the component lightweight and dependency-free |
| BR-4 | **No network protocols** | FlowForge is an in-process engine, not a service |
| BR-5 | **No scheduling (cron-like)** | Trigger mechanisms are the host application's responsibility |
| BR-6 | **English-only API** | Method names, exceptions, and docs in English |

---

## 5. Class Diagram

```mermaid
classDiagram
    class WorkflowBuilder {
        -_name: str
        -_steps: List~Dict~
        -_event_bus: EventBus
        -_checkpoint_manager: CheckpointManager
        -_max_workers: int
        -_fail_fast: bool
        +add_step(step_id, func, ...) WorkflowBuilder
        +max_workers(n) WorkflowBuilder
        +fail_fast(enabled) WorkflowBuilder
        +on_start(callback) WorkflowBuilder
        +on_complete(callback) WorkflowBuilder
        +on_failure(callback) WorkflowBuilder
        +build() Workflow
    }

    class Workflow {
        +dag: DAG
        +engine: WorkflowEngine
        +run(context) WorkflowResult
        +pause(context) str
        +resume(checkpoint_id) WorkflowResult
        +cancel() void
    }

    class DAG {
        +name: str
        +description: str
        -_nodes: Dict~str, Node~
        -_adjacency: Dict~str, List~str~~
        -_reverse: Dict~str, List~str~~
        +add_node(node) DAG
        +add_edge(from_id, to_id) DAG
        +get_node(node_id) Node
        +remove_node(node_id) void
        +topological_sort() List~str~
        +get_ready_nodes() List~Node~
        +get_root_nodes() List~Node~
        +get_leaf_nodes() List~Node~
        +validate() List~str~
        +reset() void
        +to_dict() dict
    }

    class Node {
        +node_id: str
        +name: str
        +func: Callable
        +status: NodeStatus
        +result: Any
        +error: Exception
        +retry_policy: RetryPolicy
        +timeout: TimeoutPolicy
        +condition: Condition
        +metrics: NodeMetrics
        +is_terminal: bool
        +is_runnable: bool
        +reset() void
        +to_dict() dict
    }

    class NodeMetrics {
        +created_at: datetime
        +started_at: datetime
        +completed_at: datetime
        +retry_count: int
        +duration_seconds: float
        +mark_started() void
        +mark_completed() void
    }

    class WorkflowEngine {
        +max_workers: int
        +event_bus: EventBus
        +checkpoint_manager: CheckpointManager
        +fail_fast: bool
        +status: WorkflowStatus
        +run(dag, context) WorkflowResult
        +pause(dag, context) str
        +resume(dag, checkpoint_id) WorkflowResult
        +cancel() void
    }

    class WorkflowResult {
        +status: WorkflowStatus
        +context: ExecutionContext
        +node_results: Dict
        +errors: Dict
        +duration_seconds: float
        +is_success: bool
    }

    class ExecutionContext {
        -_data: Dict
        -_node_results: Dict
        -_lock: Lock
        +workflow_id: str
        +set(key, value) void
        +get(key, default) Any
        +set_node_result(node_id, result) void
        +get_node_result(node_id) Any
        +snapshot() dict
        +from_snapshot(snap) ExecutionContext
    }

    class EventBus {
        -_listeners: Dict
        -_global_listeners: List
        -_history: List~Event~
        +on(event_type, callback) EventBus
        +on_any(callback) EventBus
        +off(event_type, callback) EventBus
        +emit(event_type, ...) Event
        +enable_history() void
    }

    class Event {
        +event_type: EventType
        +node_id: str
        +data: dict
    }

    class CheckpointManager {
        -_checkpoints: Dict~str, Checkpoint~
        +save(dag, context) str
        +restore(checkpoint_id) tuple
        +list_checkpoints() List~Checkpoint~
        +delete(checkpoint_id) bool
        +clear() int
    }

    class Checkpoint {
        +checkpoint_id: str
        +workflow_name: str
        +dag_state: dict
        +context_snapshot: dict
        +created_at: datetime
    }

    class RetryPolicy {
        <<abstract>>
        +max_retries: int
        +retry_on: tuple
        +should_retry(attempt, error) bool
        +get_delay(attempt) float*
        +strategy: RetryStrategy*
    }

    class NoRetryPolicy {
        +should_retry() bool
        +get_delay() float
    }

    class FixedRetryPolicy {
        +delay: float
        +get_delay(attempt) float
    }

    class ExponentialBackoffPolicy {
        +base_delay: float
        +max_delay: float
        +get_delay(attempt) float
    }

    class LinearBackoffPolicy {
        +base_delay: float
        +max_delay: float
        +get_delay(attempt) float
    }

    class TimeoutPolicy {
        +timeout_seconds: float
        +execute(func, ...) Any
    }

    class Condition {
        <<abstract>>
        +evaluate(context) bool*
    }

    class LambdaCondition {
        +evaluate(context) bool
    }

    class ResultCondition {
        +source_node_id: str
        +expected_value: Any
        +operator: str
        +evaluate(context) bool
    }

    class AlwaysTrue {
        +evaluate(context) bool
    }

    class AlwaysFalse {
        +evaluate(context) bool
    }

    %% Relationships
    WorkflowBuilder --> Workflow : builds
    Workflow --> DAG : contains
    Workflow --> WorkflowEngine : uses
    DAG "1" --> "*" Node : contains
    Node --> NodeMetrics : has
    Node --> RetryPolicy : uses
    Node --> TimeoutPolicy : uses
    Node --> Condition : gated by
    WorkflowEngine --> DAG : executes
    WorkflowEngine --> ExecutionContext : manages
    WorkflowEngine --> EventBus : emits to
    WorkflowEngine --> CheckpointManager : persists via
    WorkflowEngine --> WorkflowResult : produces
    CheckpointManager --> Checkpoint : stores
    EventBus --> Event : creates
    RetryPolicy <|-- NoRetryPolicy
    RetryPolicy <|-- FixedRetryPolicy
    RetryPolicy <|-- ExponentialBackoffPolicy
    RetryPolicy <|-- LinearBackoffPolicy
    Condition <|-- LambdaCondition
    Condition <|-- ResultCondition
    Condition <|-- AlwaysTrue
    Condition <|-- AlwaysFalse
```

---

## 6. Sequence Diagrams

### 6.1 Normal Workflow Execution

```mermaid
sequenceDiagram
    participant User
    participant Builder as WorkflowBuilder
    participant WF as Workflow
    participant Engine as WorkflowEngine
    participant DAG
    participant Node
    participant Ctx as ExecutionContext
    participant Bus as EventBus

    User->>Builder: add_step("extract", fn)
    User->>Builder: add_step("transform", fn, depends_on=["extract"])
    User->>Builder: build()
    Builder->>DAG: add_node(), add_edge()
    Builder->>DAG: validate()
    Builder-->>User: Workflow

    User->>WF: run()
    WF->>Engine: run(dag, context)
    Engine->>DAG: validate()
    Engine->>Bus: emit(WORKFLOW_STARTED)

    loop For each batch of ready nodes
        Engine->>DAG: get_ready_nodes()
        DAG-->>Engine: [ready nodes]

        par Execute ready nodes in parallel
            Engine->>Node: evaluate condition
            Node-->>Engine: True
            Engine->>Bus: emit(NODE_STARTED)
            Engine->>Node: func(context)
            Node-->>Engine: result
            Engine->>Ctx: set_node_result(id, result)
            Engine->>Bus: emit(NODE_COMPLETED)
        end
    end

    Engine->>Bus: emit(WORKFLOW_COMPLETED)
    Engine-->>WF: WorkflowResult
    WF-->>User: WorkflowResult
```

### 6.2 Retry Flow

```mermaid
sequenceDiagram
    participant Engine as WorkflowEngine
    participant Node
    participant Policy as RetryPolicy
    participant Bus as EventBus

    Engine->>Node: func(context)
    Node-->>Engine: raises Exception

    Engine->>Policy: should_retry(attempt=1, error)
    Policy-->>Engine: True

    Engine->>Bus: emit(NODE_RETRYING, attempt=1)
    Engine->>Policy: get_delay(attempt=1)
    Policy-->>Engine: 1.0s

    Note over Engine: sleep(1.0s)

    Engine->>Node: func(context)  [retry]
    Node-->>Engine: raises Exception

    Engine->>Policy: should_retry(attempt=2, error)
    Policy-->>Engine: True

    Engine->>Bus: emit(NODE_RETRYING, attempt=2)
    Engine->>Policy: get_delay(attempt=2)
    Policy-->>Engine: 2.0s

    Note over Engine: sleep(2.0s)

    Engine->>Node: func(context)  [retry]
    Node-->>Engine: result (success!)

    Engine->>Bus: emit(NODE_COMPLETED)
```

### 6.3 Checkpoint / Resume Flow

```mermaid
sequenceDiagram
    participant User
    participant Engine as WorkflowEngine
    participant CpMgr as CheckpointManager
    participant DAG
    participant Ctx as ExecutionContext
    participant Bus as EventBus

    Note over Engine: Workflow running...
    Note over Engine: Nodes A, B completed

    User->>Engine: pause(dag, context)
    Engine->>Bus: emit(WORKFLOW_PAUSED)
    Engine->>DAG: to_dict()
    DAG-->>Engine: dag_state
    Engine->>Ctx: snapshot()
    Ctx-->>Engine: context_snapshot
    Engine->>CpMgr: save(dag_state, context_snapshot)
    CpMgr-->>Engine: checkpoint_id
    Engine->>Bus: emit(CHECKPOINT_SAVED)
    Engine-->>User: checkpoint_id

    Note over User: Time passes...

    User->>Engine: resume(dag, checkpoint_id)
    Engine->>CpMgr: restore(checkpoint_id)
    CpMgr-->>Engine: (dag_state, context_snapshot)
    Engine->>DAG: restore node statuses
    Engine->>Ctx: from_snapshot(context_snapshot)
    Engine->>Bus: emit(CHECKPOINT_RESTORED)
    Engine->>Engine: run(dag, restored_context)
    Note over Engine: Continues from node C...
    Engine-->>User: WorkflowResult
```

### 6.4 Conditional Branching Flow

```mermaid
sequenceDiagram
    participant Engine as WorkflowEngine
    participant NodeV as validate
    participant NodeS as success_path
    participant NodeF as failure_path
    participant Cond as ResultCondition
    participant Ctx as ExecutionContext
    participant Bus as EventBus

    Engine->>NodeV: func(context)
    NodeV-->>Engine: True
    Engine->>Ctx: set_node_result("validate", True)
    Engine->>Bus: emit(NODE_COMPLETED, "validate")

    par Evaluate conditional branches
        Engine->>Cond: evaluate(context) for success_path
        Cond->>Ctx: get_node_result("validate")
        Ctx-->>Cond: True
        Cond-->>Engine: True (match!)
        Engine->>NodeS: func(context)
        NodeS-->>Engine: "success!"
        Engine->>Bus: emit(NODE_COMPLETED, "success_path")
    and
        Engine->>Cond: evaluate(context) for failure_path
        Cond->>Ctx: get_node_result("validate")
        Ctx-->>Cond: True
        Cond-->>Engine: False (no match)
        Engine->>Bus: emit(NODE_SKIPPED, "failure_path")
    end
```

---

## 7. State Diagrams

### 7.1 Node Lifecycle

```mermaid
stateDiagram-v2
    [*] --> PENDING : Node created

    PENDING --> READY : Dependencies satisfied
    PENDING --> SKIPPED : Condition = False

    READY --> RUNNING : Engine dispatches

    RUNNING --> COMPLETED : func() returns
    RUNNING --> FAILED : func() raises & retries exhausted
    RUNNING --> RETRYING : func() raises & retries remaining

    RETRYING --> RUNNING : After delay

    COMPLETED --> [*]
    FAILED --> [*]
    SKIPPED --> [*]

    note right of PENDING
        Initial state.
        Waiting for parent
        nodes to complete.
    end note

    note right of RETRYING
        Delay computed by
        RetryPolicy.get_delay()
    end note
```

### 7.2 Workflow Lifecycle

```mermaid
stateDiagram-v2
    [*] --> CREATED : WorkflowEngine instantiated

    CREATED --> RUNNING : engine.run() called

    RUNNING --> COMPLETED : All nodes completed/skipped
    RUNNING --> FAILED : Node failed fatally (fail_fast=True)
    RUNNING --> PAUSED : engine.pause() called
    RUNNING --> CANCELLED : engine.cancel() called

    PAUSED --> RUNNING : engine.resume() called

    COMPLETED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]

    note right of PAUSED
        Checkpoint saved.
        State can be restored.
    end note
```

---

## 8. Deployment Diagram

```mermaid
graph TB
    subgraph HostApplication["Host Application (Python Process)"]
        subgraph UserCode["User Code"]
            Steps["Step Functions<br/>(Business Logic)"]
            Config["Workflow Definition<br/>(Builder / Decorators)"]
        end

        subgraph FlowForge["FlowForge Library"]
            subgraph Core["Core Engine"]
                DAG["DAG<br/>Graph Structure"]
                Engine["WorkflowEngine<br/>Scheduler"]
                Context["ExecutionContext<br/>Shared State"]
            end

            subgraph Policies["Policies"]
                Retry["RetryPolicy"]
                Timeout["TimeoutPolicy"]
            end

            subgraph Features["Features"]
                Branch["Condition<br/>Branching"]
                Events["EventBus<br/>Pub/Sub"]
                CP["CheckpointManager<br/>State Persistence"]
            end

            subgraph Builders["Construction"]
                Builder["WorkflowBuilder"]
                Decorator["@workflow_step"]
            end
        end

        subgraph Runtime["Python Runtime"]
            ThreadPool["ThreadPoolExecutor<br/>(concurrent.futures)"]
            Threading["threading.Lock<br/>(Thread Safety)"]
            StdLib["Standard Library<br/>(copy, uuid, datetime)"]
        end
    end

    Config --> Builder
    Config --> Decorator
    Builder --> DAG
    Decorator --> Builder
    Steps --> Engine
    DAG --> Engine
    Engine --> Context
    Engine --> Retry
    Engine --> Timeout
    Engine --> Branch
    Engine --> Events
    Engine --> CP
    Engine --> ThreadPool
    Context --> Threading

    style FlowForge fill:#1a1a2e,stroke:#e94560,stroke-width:2px,color:#eee
    style Core fill:#16213e,stroke:#0f3460,color:#eee
    style Policies fill:#16213e,stroke:#0f3460,color:#eee
    style Features fill:#16213e,stroke:#0f3460,color:#eee
    style Builders fill:#16213e,stroke:#0f3460,color:#eee
    style HostApplication fill:#0f0f1a,stroke:#533483,stroke-width:2px,color:#eee
    style Runtime fill:#1a1a2e,stroke:#533483,color:#eee
    style UserCode fill:#1a1a2e,stroke:#533483,color:#eee
```

---

## 9. Design Patterns Used

| Pattern | Where | Purpose |
|---------|-------|---------|
| **Builder** | `WorkflowBuilder` | Fluent construction of complex DAG + Engine configuration |
| **Strategy** | `RetryPolicy` hierarchy | Interchangeable retry delay algorithms |
| **Observer** | `EventBus` | Decouple lifecycle monitoring from execution logic |
| **Template Method** | `WorkflowEngine._execute_node()` | Fixed algorithm with extension points (condition, retry, timeout) |
| **Memento** | `CheckpointManager` / `Checkpoint` | Capture and restore workflow state without violating encapsulation |
| **Decorator** | `@workflow_step` | Transparently augment functions with step metadata |
| **Chain of Responsibility** | Condition composites (`AND`, `OR`, `NOT`) | Composable condition evaluation chains |
| **Facade** | `Workflow` class | Simplified interface over DAG + Engine + Context |

---

## 10. Package Dependency Graph

```mermaid
graph TD
    init["flowforge/__init__.py"]
    enums["enums.py"]
    exceptions["exceptions.py"]
    node["core/node.py"]
    dag["core/dag.py"]
    context["core/context.py"]
    engine["core/engine.py"]
    retry["policies/retry.py"]
    timeout["policies/timeout.py"]
    conditions["branching/conditions.py"]
    hooks["events/hooks.py"]
    checkpoint["checkpoint/manager.py"]
    builder["builders/workflow.py"]

    init --> node
    init --> dag
    init --> context
    init --> engine
    init --> retry
    init --> timeout
    init --> conditions
    init --> hooks
    init --> checkpoint
    init --> builder

    node --> enums
    dag --> node
    dag --> enums
    dag --> exceptions
    engine --> dag
    engine --> node
    engine --> context
    engine --> enums
    engine --> hooks
    engine --> checkpoint
    engine --> exceptions
    engine --> retry

    retry --> enums
    timeout --> exceptions
    conditions -.-> context
    hooks --> enums
    checkpoint --> exceptions
    checkpoint -.-> dag
    checkpoint -.-> context
    builder --> dag
    builder --> engine
    builder --> node
    builder --> hooks
    builder --> checkpoint
    builder --> exceptions

    style init fill:#e94560,stroke:#fff,color:#fff
    style engine fill:#0f3460,stroke:#fff,color:#fff
    style dag fill:#0f3460,stroke:#fff,color:#fff
```

---

## 11. Thread Safety Analysis

| Component | Thread Safety Mechanism | Notes |
|-----------|------------------------|-------|
| `ExecutionContext` | `threading.Lock` on all `_data` and `_node_results` access | All public methods are thread-safe |
| `WorkflowEngine` | `threading.Lock` on `_errors` dict | Cancel/pause use `threading.Event` |
| `EventBus` | Listeners invoked synchronously within the emitting thread | Error-safe: listener exceptions are caught and logged |
| `Node` | Mutable state accessed only by the engine's scheduling loop | Each node is dispatched to at most one thread at a time |
| `CheckpointManager` | Not thread-safe by design | Intended to be called from the engine (single-threaded control flow) |

---

*Document generated for FlowForge v1.0.0*
