"""
Activities: flag_for_human_review, record_final_decision

flag_for_human_review:
  Updates the application status to "pending_review" and logs the
  AI recommendation to MongoDB so the loan officer has context.

record_final_decision:
  Writes the final approved/denied decision (and the deciding officer)
  to MongoDB, closing out the loan application record.
"""

from datetime import datetime, timezone

from temporalio import activity

from .db import get_collection


@activity.defn
async def flag_for_human_review(application: dict) -> dict:
    """
    Mark the application as awaiting human review in MongoDB.
    Logs the AI recommendation so the loan officer can see it
    when they look up the record in Atlas.

    Args:
        application: Application dict with `ai_analysis`

    Returns:
        The same application dict (unchanged — status update is in Mongo)
    """
    app_id = application["_id"]
    ai = application.get("ai_analysis", {})

    activity.logger.info(
        f"[{app_id}] Flagging for human review. "
        f"AI recommendation: {ai.get('recommendation', 'N/A')} "
        f"(risk score: {ai.get('risk_score', 'N/A')})"
    )

    collection = await get_collection("loan_applications")
    await collection.update_one(
        {"_id": app_id},
        {
            "$set": {
                "status": "pending_review",
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )

    activity.logger.info(f"[{app_id}] Status set to pending_review in MongoDB Atlas")
    return application


@activity.defn
async def record_final_decision(decision_data: dict) -> dict:
    """
    Write the loan officer's final decision to MongoDB.

    Args:
        decision_data: Dict with keys:
          - application_id: str
          - decision: "approved" | "denied"
          - decided_by: str (officer identifier)

    Returns:
        Dict with application_id, decision, decided_by, decided_at
    """
    app_id = decision_data["application_id"]
    decision = decision_data["decision"]
    decided_by = decision_data["decided_by"]
    decided_at = datetime.now(timezone.utc)

    activity.logger.info(
        f"[{app_id}] Recording final decision: {decision} by {decided_by}"
    )

    collection = await get_collection("loan_applications")
    await collection.update_one(
        {"_id": app_id},
        {
            "$set": {
                "status": "decided",
                "decision": {
                    "outcome": decision,
                    "decided_by": decided_by,
                    "decided_at": decided_at,
                },
                "updated_at": decided_at,
            }
        },
    )

    activity.logger.info(
        f"[{app_id}] Decision written to MongoDB Atlas — {decision.upper()} ✓"
    )

    return {
        "application_id": app_id,
        "decision": decision,
        "decided_by": decided_by,
        "decided_at": decided_at.isoformat(),
    }
