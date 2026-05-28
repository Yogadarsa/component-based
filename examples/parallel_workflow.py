"""
FlowForge Example: Parallel Fan-Out / Fan-In Workflow
=======================================================
Demonstrates parallel execution with independent processing streams
that converge at an aggregation point.

Pattern:
          fetch_data
         /    |    \
    process  process  process
     chunk0  chunk1   chunk2
         \    |    /
         aggregate
"""

import time
from flowforge import WorkflowBuilder


def fetch_data(ctx):
    """Simulate fetching a large dataset."""
    print("  [Fetch] Downloading dataset...")
    time.sleep(0.05)  # Simulate network I/O
    data = list(range(1, 31))  # 30 items
    print(f"  [Fetch] Downloaded {len(data)} items")
    return data


def make_processor(chunk_index, chunk_size=10):
    """Factory: create a processor for a specific data chunk."""

    def process(ctx):
        data = ctx.get_node_result("fetch")
        start = chunk_index * chunk_size
        end = start + chunk_size
        chunk = data[start:end]

        print(f"  [Process {chunk_index}] Processing items {start+1}-{end}...")
        time.sleep(0.05)  # Simulate CPU work

        result = sum(x ** 2 for x in chunk)
        print(f"  [Process {chunk_index}] Sum of squares = {result}")
        return result

    return process


def aggregate(ctx):
    """Combine results from all parallel processors."""
    results = []
    for i in range(3):
        r = ctx.get_node_result(f"process_{i}")
        results.append(r)

    total = sum(results)
    print(f"  [Aggregate] Combining {len(results)} partial results...")
    print(f"  [Aggregate] Partial: {results}")
    print(f"  [Aggregate] Total sum of squares = {total}")
    return {"partials": results, "total": total}


def main():
    print("=" * 60)
    print("FlowForge Example: Parallel Fan-Out / Fan-In")
    print("=" * 60)

    # Build workflow with parallel branches
    workflow = (
        WorkflowBuilder("parallel_processing")
        .add_step("fetch", fetch_data)
        .add_step("process_0", make_processor(0), depends_on=["fetch"])
        .add_step("process_1", make_processor(1), depends_on=["fetch"])
        .add_step("process_2", make_processor(2), depends_on=["fetch"])
        .add_step(
            "aggregate",
            aggregate,
            depends_on=["process_0", "process_1", "process_2"],
        )
        .max_workers(3)  # Enable parallel execution
        .build()
    )

    print(f"\nWorkflow: {workflow.name}")
    print(f"Max parallel workers: 3")
    print()

    # Run and time
    start = time.time()
    result = workflow.run()
    wall_time = time.time() - start

    # Print results
    print()
    print("-" * 60)
    print(f"Status:    {result.status.name}")
    print(f"Duration:  {result.duration_seconds:.3f}s (wall: {wall_time:.3f}s)")
    agg = result.node_results["aggregate"]
    print(f"Partials:  {agg['partials']}")
    print(f"Total:     {agg['total']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
