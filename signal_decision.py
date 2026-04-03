"""
Loan Officer Decision Signal

Sends a Temporal Signal to a running LoanApplicationWorkflow,
providing the loan officer's final approved/denied decision.

This unblocks the workflow from its wait_condition and triggers
the record_final_decision activity.

Usage:
    python signal_decision.py --workflow-id loan-app-abc123 --decision approved
    python signal_decision.py --workflow-id loan-app-abc123 --decision denied --officer officer_42

The workflow will resume within seconds of receiving the signal.
"""

import argparse
import asyncio
import os

from dotenv import load_dotenv
from temporalio.client import Client

from workflows.loan_workflow import LoanApplicationWorkflow, TASK_QUEUE

load_dotenv()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Send loan decision signal to a workflow")
    parser.add_argument(
        "--workflow-id",
        required=True,
        help="The workflow ID printed by client.py",
    )
    parser.add_argument(
        "--decision",
        required=True,
        choices=["approved", "denied"],
        help="The loan officer's decision",
    )
    parser.add_argument(
        "--officer",
        default="officer_01",
        help="Identifier for the loan officer (default: officer_01)",
    )
    args = parser.parse_args()

    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")

    client = await Client.connect(address, namespace=namespace)
    handle = client.get_workflow_handle(args.workflow_id)

    print(f"\n[Temporal] Sending signal to workflow: {args.workflow_id}")
    print(f"  Decision : {args.decision}")
    print(f"  Officer  : {args.officer}\n")

    await handle.signal(
        LoanApplicationWorkflow.submit_loan_decision,
        args=[args.decision, args.officer],
    )

    print(f"[Temporal] Signal sent ✓")
    print(f"  The workflow will resume and record the decision in MongoDB Atlas.")
    print()
    print(f"To see the final result, wait a moment then check MongoDB Atlas:")
    print(f"  Collection  : loan_applications")
    print(f"  Filter      : {{ \"_id\": \"{args.workflow_id}\" }}")
    print(f"  Field       : decision.outcome")


if __name__ == "__main__":
    asyncio.run(main())
