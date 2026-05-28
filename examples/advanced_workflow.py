"""
FlowForge Example: Advanced Workflow
======================================
Demonstrates all FlowForge features working together:
- Retry policies (exponential backoff)
- Event hooks (logging, alerting)
- Checkpointing (save/restore)
- Conditional branching
- Parallel execution
- Shared context

Scenario: Data processing pipeline with error recovery.
"""

import time
from flowforge import (
    WorkflowBuilder,
    ExecutionContext,
    EventBus,
    EventType,
    CheckpointManager,
    ExponentialBackoffPolicy,
    FixedRetryPolicy,
    TimeoutPolicy,
    ResultCondition,
    LambdaCondition,
    NodeStatus,
)


# ── Event Monitoring ─────────────────────────────────────────────────

def create_monitored_bus():
    """Create an event bus with rich logging."""
    bus = EventBus()
    bus.enable_history()

    def on_workflow_start(event):
        print(f"  [*] Workflow '{event.data.get('workflow')}' started")

    def on_node_start(event):
        print(f"  [>] Node '{event.node_id}' started")

    def on_node_complete(event):
        dur = event.data.get("duration", 0)
        print(f"  [OK] Node '{event.node_id}' completed ({dur:.3f}s)")

    def on_node_retry(event):
        attempt = event.data.get("attempt", 0)
        err = event.data.get("error", "")
        print(f"  [RETRY] Node '{event.node_id}' retrying (attempt {attempt}): {err}")

    def on_node_failed(event):
        print(f"  [FAIL] Node '{event.node_id}' FAILED: {event.data.get('error')}")

    def on_node_skipped(event):
        print(f"  [SKIP] Node '{event.node_id}' skipped")

    def on_workflow_complete(event):
        dur = event.data.get("duration", 0)
        print(f"  [DONE] Workflow completed in {dur:.3f}s")

    def on_workflow_failed(event):
        errors = event.data.get("errors", {})
        print(f"  [ERROR] Workflow FAILED with {len(errors)} error(s)")

    bus.on(EventType.WORKFLOW_STARTED, on_workflow_start)
    bus.on(EventType.NODE_STARTED, on_node_start)
    bus.on(EventType.NODE_COMPLETED, on_node_complete)
    bus.on(EventType.NODE_RETRYING, on_node_retry)
    bus.on(EventType.NODE_FAILED, on_node_failed)
    bus.on(EventType.NODE_SKIPPED, on_node_skipped)
    bus.on(EventType.WORKFLOW_COMPLETED, on_workflow_complete)
    bus.on(EventType.WORKFLOW_FAILED, on_workflow_failed)

    return bus


# ── Workflow Steps ───────────────────────────────────────────────────

# Simulate a flaky API call that fails the first time
_api_call_count = {"n": 0}


def fetch_from_api(ctx):
    """Fetch data from an external API (simulates transient failure)."""
    _api_call_count["n"] += 1
    if _api_call_count["n"] == 1:
        raise ConnectionError("API timeout - retrying...")
    return {
        "users": ["Alice", "Bob", "Charlie", "Diana"],
        "source": "external_api",
    }


def validate_data(ctx):
    """Validate the fetched data."""
    data = ctx.get_node_result("fetch")
    is_valid = len(data["users"]) > 0
    ctx.set("validation_passed", is_valid)
    return is_valid


def enrich_user(index):
    """Factory: enrich a specific user."""
    def _enrich(ctx):
        data = ctx.get_node_result("fetch")
        user = data["users"][index]
        time.sleep(0.02)  # Simulate API call
        return {"name": user, "email": f"{user.lower()}@example.com", "enriched": True}
    return _enrich


def aggregate_users(ctx):
    """Combine enriched user profiles."""
    profiles = []
    for i in range(4):
        profile = ctx.get_node_result(f"enrich_{i}")
        if profile:
            profiles.append(profile)
    return profiles


def generate_report(ctx):
    """Generate a summary report."""
    profiles = ctx.get_node_result("aggregate")
    report = {
        "total_users": len(profiles),
        "emails": [p["email"] for p in profiles],
        "source": ctx.get_node_result("fetch")["source"],
    }
    return report


def send_notifications(ctx):
    """Send notifications (only if validation passed)."""
    report = ctx.get_node_result("report")
    return f"Notified {report['total_users']} users"


def handle_invalid_data(ctx):
    """Handle the case where data validation fails."""
    return "Alert: Invalid data received - manual review required"


def main():
    print("=" * 60)
    print("FlowForge Example: Advanced Multi-Feature Workflow")
    print("=" * 60)
    print()

    # Reset the flaky API counter
    _api_call_count["n"] = 0

    # Create monitored event bus and checkpoint manager
    bus = create_monitored_bus()
    cp_mgr = CheckpointManager()

    # Build the advanced workflow
    workflow = (
        WorkflowBuilder("advanced_pipeline", description="Full-featured demo")
        .event_bus(bus)
        .checkpoint_manager(cp_mgr)
        .max_workers(4)

        # Step 1: Fetch with retry (exponential backoff)
        .add_step(
            "fetch",
            fetch_from_api,
            name="Fetch from API",
            retry=ExponentialBackoffPolicy(max_retries=3, base_delay=0.05, max_delay=1.0),
        )

        # Step 2: Validate
        .add_step(
            "validate",
            validate_data,
            name="Validate Data",
            depends_on=["fetch"],
        )

        # Step 3: Parallel enrichment (4 users in parallel)
        .add_step("enrich_0", enrich_user(0), depends_on=["validate"],
                  condition=ResultCondition("validate", expected_value=True))
        .add_step("enrich_1", enrich_user(1), depends_on=["validate"],
                  condition=ResultCondition("validate", expected_value=True))
        .add_step("enrich_2", enrich_user(2), depends_on=["validate"],
                  condition=ResultCondition("validate", expected_value=True))
        .add_step("enrich_3", enrich_user(3), depends_on=["validate"],
                  condition=ResultCondition("validate", expected_value=True))

        # Step 4: Aggregate
        .add_step(
            "aggregate",
            aggregate_users,
            depends_on=["enrich_0", "enrich_1", "enrich_2", "enrich_3"],
            condition=ResultCondition("validate", expected_value=True),
        )

        # Step 5: Generate report
        .add_step(
            "report",
            generate_report,
            depends_on=["aggregate"],
            condition=ResultCondition("validate", expected_value=True),
        )

        # Step 6: Send notifications
        .add_step(
            "notify",
            send_notifications,
            depends_on=["report"],
            condition=ResultCondition("validate", expected_value=True),
        )

        # Alternative path: Handle invalid data
        .add_step(
            "handle_invalid",
            handle_invalid_data,
            depends_on=["validate"],
            condition=ResultCondition("validate", expected_value=False),
        )

        .build()
    )

    print(f"Workflow: {workflow.name}")
    print(f"Nodes:   {workflow.dag.node_count}")
    print(f"Edges:   {workflow.dag.edge_count}")
    print()

    # Run the workflow
    result = workflow.run()

    # Print summary
    print()
    print("-" * 60)
    print(f"Status:   {result.status.name}")
    print(f"Duration: {result.duration_seconds:.3f}s")
    print(f"Completed nodes: {len(result.node_results)}")
    print(f"Failed nodes:    {len(result.errors)}")

    if result.is_success:
        report = result.node_results.get("report", {})
        notify = result.node_results.get("notify", "")
        print(f"\nReport:  {report}")
        print(f"Notify:  {notify}")

    # Show event history
    print(f"\nEvent history ({len(bus.history)} events):")
    for event in bus.history:
        print(f"  [{event.event_type.name}] node={event.node_id or 'workflow'}")

    print("=" * 60)


if __name__ == "__main__":
    main()
