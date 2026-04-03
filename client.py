"""
Loan Application Submitter

Constructs a sample loan application and starts a new
LoanApplicationWorkflow in Temporal.

Usage:
    python client.py
    python client.py --workflow-id my-custom-id
    python client.py --applicant "John Doe" --amount 50000 --purpose "business expansion"

After running, open the Temporal Web UI at http://localhost:8233 (or your
Temporal Cloud URL) to watch the workflow progress in real time.
"""

import argparse
import asyncio
import os
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from temporalio.client import Client

from workflows.loan_workflow import LoanApplicationWorkflow, TASK_QUEUE

load_dotenv()


def build_application(workflow_id: str, args: argparse.Namespace) -> dict:
    """Construct the initial loan application document."""
    return {
        "_id": workflow_id,
        "workflow_id": workflow_id,
        "status": "submitted",
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "borrower": {
            "name": args.applicant,
            "annual_income": args.income,
            "credit_score": args.credit_score,
            "employment_status": args.employment,
        },
        "loan": {
            "amount": args.amount,
            "purpose": args.purpose,
            "term_months": args.term_months,
        },
        "documents": [
            {
                "type": "pay_stub",
                "text": (
                    f"Pay stub for {args.applicant}. "
                    f"Employer: Acme Corp. Monthly gross pay: ${args.income // 12:,}. "
                    f"YTD earnings: ${args.income:,}. Employment status: {args.employment}."
                ),
            },
            {
                "type": "loan_purpose_statement",
                "text": (
                    f"I am requesting a ${args.amount:,} loan for {args.purpose}. "
                    f"I have been employed for 4 years and my monthly expenses are "
                    f"approximately ${(args.income // 12) // 3:,}. "
                    f"I plan to repay over {args.term_months} months."
                ),
            },
        ],
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Submit a loan application")
    parser.add_argument("--workflow-id", default=f"loan-app-{uuid.uuid4().hex[:8]}")
    parser.add_argument("--applicant", default="Jane Smith")
    parser.add_argument("--income", type=int, default=95_000)
    parser.add_argument("--credit-score", type=int, default=720)
    parser.add_argument("--employment", default="employed")
    parser.add_argument("--amount", type=int, default=25_000)
    parser.add_argument("--purpose", default="home renovation")
    parser.add_argument("--term-months", type=int, default=60)
    args = parser.parse_args()

    application = build_application(args.workflow_id, args)

    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")

    client = await Client.connect(address, namespace=namespace)

    print(f"\n[Temporal] Starting workflow: {args.workflow_id}")
    print(f"  Applicant : {args.applicant}")
    print(f"  Loan      : ${args.amount:,} over {args.term_months} months")
    print(f"  Purpose   : {args.purpose}")
    print(f"  Income    : ${args.income:,} | Credit score: {args.credit_score}\n")

    handle = await client.start_workflow(
        LoanApplicationWorkflow.run,
        application,
        id=args.workflow_id,
        task_queue=TASK_QUEUE,
    )

    print(f"[Temporal] Workflow started ✓")
    print(f"  Workflow ID : {handle.id}")
    print(f"  Run ID      : {handle.result_run_id}")
    print()
    print(f"Watch progress in the Temporal Web UI:")
    print(f"  http://localhost:8233/namespaces/{namespace}/workflows/{handle.id}")
    print()
    print(f"Once the workflow reaches 'pending_review', approve or deny with:")
    print(f"  python signal_decision.py --workflow-id {handle.id} --decision approved")


if __name__ == "__main__":
    asyncio.run(main())
