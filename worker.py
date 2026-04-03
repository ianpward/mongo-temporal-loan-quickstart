"""
Temporal Worker

Registers the LoanApplicationWorkflow and all activities,
then polls the "loan-processing" task queue for work.

Usage:
    python worker.py

The worker must be running before you submit an application.
To demonstrate fault tolerance, kill this process mid-workflow
(Ctrl+C after Stage 2 completes), then restart it — the workflow
will resume from exactly where it left off.
"""

import asyncio
import logging
import os

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.worker import Worker

from activities.credit_analysis import run_credit_analysis
from activities.extract_documents import extract_and_embed_documents
from activities.record_decision import flag_for_human_review, record_final_decision
from activities.retrieve_similar import retrieve_similar_applications
from workflows.loan_workflow import LoanApplicationWorkflow, TASK_QUEUE

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


async def main() -> None:
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")

    logging.info(f"Connecting to Temporal at {address} (namespace: {namespace})")
    client = await Client.connect(address, namespace=namespace)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[LoanApplicationWorkflow],
        activities=[
            extract_and_embed_documents,
            retrieve_similar_applications,
            run_credit_analysis,
            flag_for_human_review,
            record_final_decision,
        ],
    )

    logging.info(f"Worker started. Polling task queue: {TASK_QUEUE!r}")
    logging.info("Press Ctrl+C to stop (try this mid-workflow to see Temporal's fault tolerance!)")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
