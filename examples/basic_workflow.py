"""
FlowForge Example: Basic Linear Workflow
==========================================
A simple 3-step ETL pipeline: Extract → Transform → Load.

This demonstrates the fundamental FlowForge concepts:
- Building a workflow with WorkflowBuilder
- Sequential dependency chains
- Data passing between nodes via ExecutionContext
"""

from flowforge import WorkflowBuilder


def extract(ctx):
    """Simulate extracting raw data from a source."""
    print("  [Extract] Fetching records from database...")
    records = [
        {"id": 1, "name": "Alice", "score": 85},
        {"id": 2, "name": "Bob", "score": 92},
        {"id": 3, "name": "Charlie", "score": 78},
    ]
    print(f"  [Extract] Retrieved {len(records)} records")
    return records


def transform(ctx):
    """Clean and enrich the extracted data."""
    raw = ctx.get_node_result("extract")
    print(f"  [Transform] Processing {len(raw)} records...")

    transformed = []
    for record in raw:
        transformed.append({
            **record,
            "name": record["name"].upper(),
            "grade": "A" if record["score"] >= 90 else "B" if record["score"] >= 80 else "C",
        })

    print(f"  [Transform] Enriched {len(transformed)} records with grades")
    return transformed


def load(ctx):
    """Simulate loading data into a destination."""
    data = ctx.get_node_result("transform")
    print(f"  [Load] Writing {len(data)} records to data warehouse...")
    for record in data:
        print(f"    -> {record['name']} | Score: {record['score']} | Grade: {record['grade']}")
    print(f"  [Load] Successfully loaded {len(data)} records")
    return {"records_loaded": len(data)}


def main():
    print("=" * 60)
    print("FlowForge Example: Basic ETL Pipeline")
    print("=" * 60)

    # Build the workflow
    workflow = (
        WorkflowBuilder("basic_etl", description="Simple ETL pipeline")
        .add_step("extract", extract)
        .add_step("transform", transform, depends_on=["extract"])
        .add_step("load", load, depends_on=["transform"])
        .build()
    )

    print(f"\nWorkflow: {workflow.name}")
    print(f"Nodes: {workflow.dag.node_count}")
    print(f"Edges: {workflow.dag.edge_count}")
    print(f"Execution order: {workflow.dag.topological_sort()}")
    print()

    # Run the workflow
    result = workflow.run()

    # Print results
    print()
    print("-" * 60)
    print(f"Status:   {result.status.name}")
    print(f"Duration: {result.duration_seconds:.3f}s")
    print(f"Results:  {result.node_results['load']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
