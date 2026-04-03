"""
Activity: extract_and_embed_documents

Embeds the loan application narrative and supporting documents using
Voyage AI, then stores the embedding in MongoDB Atlas.

After this activity the application document in Atlas will have:
  - status: "embedding_complete"
  - embedding: [float, ...] (1024 dimensions)
  - narrative: str  (the combined text that was embedded)
"""

import os
from datetime import datetime, timezone

import voyageai
from motor.motor_asyncio import AsyncIOMotorClient
from temporalio import activity

from .db import get_collection


@activity.defn
async def extract_and_embed_documents(application: dict) -> dict:
    """
    Build a loan narrative from application fields, embed it with
    voyage-finance-2, and persist the embedding to MongoDB.

    Args:
        application: Full application dict (see client.py for shape)

    Returns:
        Updated application dict with `narrative` field added.
        The embedding itself lives in MongoDB to keep the Temporal
        payload small.
    """
    app_id = application["_id"]
    activity.logger.info(f"[{app_id}] Building loan narrative")

    # ------------------------------------------------------------------
    # Step 1: Construct a rich text narrative for embedding
    # We combine structured fields into prose so the embedding captures
    # the full context — income, purpose, employment, document text.
    # ------------------------------------------------------------------
    borrower = application["borrower"]
    loan = application["loan"]

    doc_texts = "\n".join(
        f"[{doc['type']}]: {doc['text']}"
        for doc in application.get("documents", [])
    )

    narrative = (
        f"Loan application for {borrower['name']}. "
        f"Requesting ${loan['amount']:,} over {loan['term_months']} months "
        f"for: {loan['purpose']}. "
        f"Borrower annual income: ${borrower['annual_income']:,}, "
        f"credit score: {borrower['credit_score']}, "
        f"employment status: {borrower['employment_status']}. "
        f"Supporting documents: {doc_texts}"
    )

    activity.logger.info(f"[{app_id}] Embedding narrative via voyage-finance-2")

    # ------------------------------------------------------------------
    # Step 2: Generate embedding via Voyage AI
    # voyage-finance-2 is optimized for financial text and produces
    # 1024-dimensional vectors suitable for Atlas Vector Search.
    # ------------------------------------------------------------------
    voyage = voyageai.AsyncClient(api_key=os.environ["VOYAGE_API_KEY"])
    result = await voyage.embed(
        texts=[narrative],
        model="voyage-finance-2",
        input_type="document",
    )
    embedding = result.embeddings[0]

    activity.logger.info(
        f"[{app_id}] Embedded {len(application.get('documents', []))} documents "
        f"via voyage-finance-2 ({len(embedding)} dimensions)"
    )

    # ------------------------------------------------------------------
    # Step 3: Persist to MongoDB Atlas
    # We store the full embedding here so Atlas Vector Search can query it.
    # ------------------------------------------------------------------
    collection = await get_collection("loan_applications")
    await collection.update_one(
        {"_id": app_id},
        {
            "$set": {
                "status": "embedding_complete",
                "narrative": narrative,
                "embedding": embedding,
                "updated_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )

    activity.logger.info(f"[{app_id}] Embedding stored in MongoDB Atlas")

    # Return application with narrative added (embedding stays in Mongo)
    return {**application, "narrative": narrative}
