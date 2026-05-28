import sys
import os
import json
from flask import Flask, jsonify, request, send_from_directory

# Ensure we can import flowforge
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Import examples (they contain pre-built workflows we can run)
import examples.basic_workflow as basic
import examples.parallel_workflow as parallel
import examples.conditional_workflow as conditional
import examples.advanced_workflow as advanced

from flowforge import WorkflowBuilder, ExecutionContext, EventBus

app = Flask(__name__, static_folder='static')

WORKFLOWS = [
    {
        "id": "basic",
        "title": "Basic ETL Pipeline",
        "description": "A simple 3-step sequential pipeline: Extract → Transform → Load.",
    },
    {
        "id": "parallel",
        "title": "Parallel Fan-Out / Fan-In",
        "description": "Independent processing streams that run concurrently and converge at an aggregation point.",
    },
    {
        "id": "conditional",
        "title": "Conditional Branching",
        "description": "If/else routing based on inventory levels (simulates an out-of-stock scenario).",
    },
    {
        "id": "advanced",
        "title": "Advanced Engine Features",
        "description": "Demonstrates retry policies (exponential backoff), parallel tasks, event monitoring, and branching combined.",
    }
]

@app.route('/')
def index():
    """Serve the frontend HTML."""
    return send_from_directory('static', 'index.html')

@app.route('/api/workflows', methods=['GET'])
def list_workflows():
    """List available workflows."""
    return jsonify({"workflows": WORKFLOWS})

@app.route('/api/run/<workflow_id>', methods=['POST'])
def run_workflow(workflow_id):
    """Run a specific workflow and return the results."""
    
    # We will intercept the event bus to collect events for the UI
    ui_bus = EventBus()
    ui_bus.enable_history()
    
    # We rebuild the workflow from the examples but inject our UI event bus
    # so we can capture the lifecycle events.
    if workflow_id == "basic":
        workflow = (
            WorkflowBuilder("basic_etl")
            .event_bus(ui_bus)
            .add_step("extract", basic.extract)
            .add_step("transform", basic.transform, depends_on=["extract"])
            .add_step("load", basic.load, depends_on=["transform"])
            .build()
        )
        ctx = ExecutionContext()
        
    elif workflow_id == "parallel":
        workflow = (
            WorkflowBuilder("parallel_processing")
            .event_bus(ui_bus)
            .add_step("fetch", parallel.fetch_data)
            .add_step("process_0", parallel.make_processor(0), depends_on=["fetch"])
            .add_step("process_1", parallel.make_processor(1), depends_on=["fetch"])
            .add_step("process_2", parallel.make_processor(2), depends_on=["fetch"])
            .add_step("aggregate", parallel.aggregate, depends_on=["process_0", "process_1", "process_2"])
            .max_workers(3)
            .build()
        )
        ctx = ExecutionContext()
        
    elif workflow_id == "conditional":
        # We'll run the out_of_stock scenario
        from flowforge import ResultCondition
        workflow = (
            WorkflowBuilder("order_out_of_stock")
            .event_bus(ui_bus)
            .add_step("validate", conditional.validate_order)
            .add_step("check_stock", conditional.check_inventory, depends_on=["validate"])
            .add_step("fulfill", conditional.fulfill_order, depends_on=["check_stock"], condition=ResultCondition("check_stock", expected_value=True))
            .add_step("backorder", conditional.create_backorder, depends_on=["check_stock"], condition=ResultCondition("check_stock", expected_value=False))
            .add_step("notify", conditional.notify_customer, depends_on=["fulfill", "backorder"])
            .build()
        )
        ctx = ExecutionContext({
            "order_id": "ORD-DEMO",
            "item": "Widget Pro",
            "quantity": 10,
            "stock_level": 2, # Forces out of stock path
        })
        
    elif workflow_id == "advanced":
        from flowforge import ExponentialBackoffPolicy, ResultCondition
        # Reset the flaky counter so it fails and retries
        advanced._api_call_count["n"] = 0
        workflow = (
            WorkflowBuilder("advanced_pipeline")
            .event_bus(ui_bus)
            .max_workers(4)
            .add_step("fetch", advanced.fetch_from_api, retry=ExponentialBackoffPolicy(max_retries=3, base_delay=0.05, max_delay=1.0))
            .add_step("validate", advanced.validate_data, depends_on=["fetch"])
            .add_step("enrich_0", advanced.enrich_user(0), depends_on=["validate"], condition=ResultCondition("validate", expected_value=True))
            .add_step("enrich_1", advanced.enrich_user(1), depends_on=["validate"], condition=ResultCondition("validate", expected_value=True))
            .add_step("enrich_2", advanced.enrich_user(2), depends_on=["validate"], condition=ResultCondition("validate", expected_value=True))
            .add_step("enrich_3", advanced.enrich_user(3), depends_on=["validate"], condition=ResultCondition("validate", expected_value=True))
            .add_step("aggregate", advanced.aggregate_users, depends_on=["enrich_0", "enrich_1", "enrich_2", "enrich_3"], condition=ResultCondition("validate", expected_value=True))
            .add_step("report", advanced.generate_report, depends_on=["aggregate"], condition=ResultCondition("validate", expected_value=True))
            .add_step("notify", advanced.send_notifications, depends_on=["report"], condition=ResultCondition("validate", expected_value=True))
            .add_step("handle_invalid", advanced.handle_invalid_data, depends_on=["validate"], condition=ResultCondition("validate", expected_value=False))
            .build()
        )
        ctx = ExecutionContext()
    else:
        return jsonify({"error": "Unknown workflow"}), 404

    # Run the workflow
    result = workflow.run(ctx)
    
    # Extract events
    events = []
    for e in ui_bus.history:
        # Avoid serialising complex exception objects for JSON safety
        data = {k: str(v) if isinstance(v, Exception) else v for k, v in e.data.items()}
        events.append({
            "event_type": e.event_type.name,
            "node_id": e.node_id,
            "data": data
        })

    # Convert results to JSON-safe dictionary
    safe_results = {}
    for node_id, res in result.node_results.items():
        try:
            json.dumps(res)
            safe_results[node_id] = res
        except TypeError:
            safe_results[node_id] = str(res)

    return jsonify({
        "status": result.status.name,
        "is_success": result.is_success,
        "duration_seconds": result.duration_seconds,
        "node_results": safe_results,
        "errors": {k: str(v) for k, v in result.errors.items()},
        "events": events
    })

if __name__ == '__main__':
    # Run the Flask app
    app.run(host='0.0.0.0', port=5000, debug=True)
