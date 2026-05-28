"""
FlowForge Example: Conditional Branching Workflow
===================================================
Demonstrates if/else routing based on runtime data.

Pattern:
    validate_order
         |
    check_inventory
       /     \
  fulfill    backorder    <- (conditional)
       \     /
      notify_customer
"""

from flowforge import (
    WorkflowBuilder,
    ExecutionContext,
    ResultCondition,
    LambdaCondition,
)


def validate_order(ctx):
    """Validate the incoming order."""
    order = {
        "id": ctx.get("order_id", "ORD-001"),
        "item": ctx.get("item", "Widget Pro"),
        "quantity": ctx.get("quantity", 5),
    }
    print(f"  [Validate] Order {order['id']}: {order['quantity']}x {order['item']}")
    return order


def check_inventory(ctx):
    """Check if the item is in stock."""
    order = ctx.get_node_result("validate")
    stock = ctx.get("stock_level", 10)
    in_stock = stock >= order["quantity"]
    print(f"  [Inventory] Stock: {stock}, Needed: {order['quantity']} -> {'IN STOCK' if in_stock else 'OUT OF STOCK'}")
    return in_stock


def fulfill_order(ctx):
    """Fulfill the order from existing inventory."""
    order = ctx.get_node_result("validate")
    print(f"  [Fulfill] Shipping {order['quantity']}x {order['item']} to customer")
    return {"action": "shipped", "order_id": order["id"]}


def create_backorder(ctx):
    """Create a backorder for out-of-stock items."""
    order = ctx.get_node_result("validate")
    print(f"  [Backorder] Creating backorder for {order['quantity']}x {order['item']}")
    return {"action": "backordered", "order_id": order["id"], "eta_days": 14}


def notify_customer(ctx):
    """Send notification to the customer."""
    # Check which path ran
    fulfillment = ctx.get_node_result("fulfill")
    backorder = ctx.get_node_result("backorder")

    if fulfillment:
        msg = f"Your order {fulfillment['order_id']} has been shipped!"
    elif backorder:
        msg = f"Your order {backorder['order_id']} is backordered (ETA: {backorder['eta_days']} days)"
    else:
        msg = "Your order status is being processed."

    print(f"  [Notify] Sending: {msg}")
    return {"notification": msg}


def run_scenario(scenario_name, stock_level, quantity):
    """Run a single scenario with given stock and quantity."""
    print(f"\n{'─' * 50}")
    print(f"Scenario: {scenario_name}")
    print(f"{'─' * 50}")

    workflow = (
        WorkflowBuilder(f"order_{scenario_name}")
        .add_step("validate", validate_order)
        .add_step("check_stock", check_inventory, depends_on=["validate"])
        .add_step(
            "fulfill",
            fulfill_order,
            depends_on=["check_stock"],
            condition=ResultCondition("check_stock", expected_value=True),
        )
        .add_step(
            "backorder",
            create_backorder,
            depends_on=["check_stock"],
            condition=ResultCondition("check_stock", expected_value=False),
        )
        .add_step(
            "notify",
            notify_customer,
            depends_on=["fulfill", "backorder"],
        )
        .build()
    )

    ctx = ExecutionContext({
        "order_id": f"ORD-{scenario_name.upper()}",
        "item": "Widget Pro",
        "quantity": quantity,
        "stock_level": stock_level,
    })

    result = workflow.run(ctx)

    print(f"\n  Result: {result.status.name}")
    print(f"  Notification: {result.node_results.get('notify', {}).get('notification', 'N/A')}")
    return result


def main():
    print("=" * 60)
    print("FlowForge Example: Conditional Branching")
    print("=" * 60)

    # Scenario 1: In stock -> fulfill
    r1 = run_scenario("in_stock", stock_level=20, quantity=5)
    assert r1.is_success

    # Scenario 2: Out of stock -> backorder
    r2 = run_scenario("out_of_stock", stock_level=2, quantity=10)
    assert r2.is_success

    print(f"\n{'=' * 60}")
    print("Both scenarios completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
