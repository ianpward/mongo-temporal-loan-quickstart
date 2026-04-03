"""
Temporal Workflow: Loan Application Processing

This Workflow orchestrates the end-to-end loan processing pipeline.
Each stage is an Activity — independently retryable if it fails.

Key Temporal concepts demonstrated:
- @workflow.defn: Declares a durable Workflow class
- @workflow.run: The main execution entry point
- @workflow.signal: Receives external events (loan officer decision)
- workflow.wait_condition: Pauses execution until a condition is met
- workflow.execute_activity: Runs an Activity with retry semantics
"""

from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

# Import activity functions for type-safe registration
with workflow.unsafe.imports_passed_through():
    from activities.extract_documents import extract_and_embed_documents
    from activities.retrieve_similar import retrieve_similar_applications
    from activities.credit_analysis import run_credit_analysis
    from activities.record_decision import flag_for_human_review, record_final_decision


TASK_QUEUE = "loan-processing"

# Retry policy applied to all activities:
# up to 3 attempts with exponential backoff, capped at 30s between retries.
DEFAULT_RETRY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
)

# Standard activity options — 5-minute timeout per activity execution.
# For LLM calls (credit_analysis) we use a longer timeout below.
ACTIVITY_OPTIONS = dict(
    start_to_close_timeout=timedelta(minutes=5),
    retry_policy=DEFAULT_RETRY,
)

LLM_ACTIVITY_OPTIONS = dict(
    start_to_close_timeout=timedelta(minutes=10),
    retry_policy=DEFAULT_RETRY,
)


@workflow.defn
class LoanApplicationWorkflow:
    """
    Orchestrates a loan application from submission through final decision.

    State machine:
        submitted → documents_embedded → similar_retrieved
            → analysis_complete → pending_review → decided
    """

    def __init__(self) -> None:
        # Signal state — set by loan officer via submit_loan_decision signal
        self._decision: str | None = None
        self._decided_by: str = "unknown"

    # -------------------------------------------------------------------------
    # Signal handler: loan officer sends approval or denial
    # -------------------------------------------------------------------------

    @workflow.signal
    async def submit_loan_decision(self, decision: str, officer_id: str = "system") -> None:
        """
        Receive a loan officer's decision from outside the workflow.

        Called by signal_decision.py:
            temporal workflow signal --signal submit_loan_decision ...

        Args:
            decision:   "approved" or "denied"
            officer_id: identifier for the officer making the decision
        """
        valid = {"approved", "denied"}
        if decision not in valid:
            raise ValueError(f"Decision must be one of {valid}, got: {decision!r}")

        workflow.logger.info(f"Signal received: {decision} by {officer_id}")
        self._decision = decision
        self._decided_by = officer_id

    # -------------------------------------------------------------------------
    # Main workflow execution
    # -------------------------------------------------------------------------

    @workflow.run
    async def run(self, application: dict) -> dict:
        """
        Execute the loan processing pipeline end-to-end.

        Args:
            application: Initial application dict (see client.py for shape)

        Returns:
            Final application dict with decision, ai_analysis, etc.
        """
        app_id = application["_id"]
        workflow.logger.info(f"[{app_id}] Workflow started")

        # ------------------------------------------------------------------
        # Stage 1: Extract and embed loan documents
        # Calls Voyage AI to embed the borrower narrative + documents,
        # then stores the embedding in MongoDB Atlas.
        # ------------------------------------------------------------------
        workflow.logger.info(f"[{app_id}] Stage 1: Extracting and embedding documents")
        application = await workflow.execute_activity(
            extract_and_embed_documents,
            application,
            **ACTIVITY_OPTIONS,
        )

        # ------------------------------------------------------------------
        # Stage 2: Retrieve semantically similar historical applications
        # Runs Atlas Vector Search to find loans with similar profiles,
        # then reranks results with Voyage rerank-2.
        # ------------------------------------------------------------------
        workflow.logger.info(f"[{app_id}] Stage 2: Retrieving similar applications")
        application = await workflow.execute_activity(
            retrieve_similar_applications,
            application,
            **ACTIVITY_OPTIONS,
        )

        # ------------------------------------------------------------------
        # Stage 3: AI-powered credit analysis via Claude
        # Passes the application + similar historical decisions to Claude,
        # which returns a risk score, recommendation, and rationale.
        # ------------------------------------------------------------------
        workflow.logger.info(f"[{app_id}] Stage 3: Running credit analysis")
        application = await workflow.execute_activity(
            run_credit_analysis,
            application,
            **LLM_ACTIVITY_OPTIONS,
        )

        # ------------------------------------------------------------------
        # Stage 4: Flag for human review
        # Updates status to "pending_review" in MongoDB and logs the
        # AI recommendation so the officer can act on it.
        # ------------------------------------------------------------------
        workflow.logger.info(f"[{app_id}] Stage 4: Flagging for human review")
        await workflow.execute_activity(
            flag_for_human_review,
            application,
            **ACTIVITY_OPTIONS,
        )

        # ------------------------------------------------------------------
        # PAUSE: Wait for loan officer signal
        # The workflow is now durably suspended — no compute is consumed.
        # It will resume the moment a signal arrives, even after a restart.
        # ------------------------------------------------------------------
        workflow.logger.info(
            f"[{app_id}] Workflow paused: awaiting loan officer signal...\n"
            f"  Run: python signal_decision.py --workflow-id {app_id} --decision approved"
        )
        await workflow.wait_condition(lambda: self._decision is not None)

        # ------------------------------------------------------------------
        # Stage 5: Record final decision
        # Writes the officer's decision + timestamp to MongoDB.
        # ------------------------------------------------------------------
        workflow.logger.info(
            f"[{app_id}] Stage 5: Recording decision — {self._decision} by {self._decided_by}"
        )
        result = await workflow.execute_activity(
            record_final_decision,
            {
                "application_id": app_id,
                "decision": self._decision,
                "decided_by": self._decided_by,
            },
            **ACTIVITY_OPTIONS,
        )

        workflow.logger.info(f"[{app_id}] Workflow complete ✓")
        return result
