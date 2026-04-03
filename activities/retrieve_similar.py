"""
Activity: retrieve_similar_applications

Finds historically similar loan applications using MongoDB Atlas Vector Search,
then reranks them by relevance using Voyage AI rerank-2.

After this activity the application document in Atlas will have:
  - status: "retrieval_complete"
  - similar_applications: [list of app IDs]
  - similar_applications_detail: [list of dicts with narrative + decision]
"""

import os
from datetime import datetime, timezone

import voyageai
from temporalio import activity

from .db import get_collection

# Number of candidates to pull from Atlas before reranking
VECTOR_SEARCH_CANDIDATES = 20
# Number of results to keep after reranking
TOP_K = 5


@activity.defn
async def retrieve_similar_applications(application: dict) -> dict:
    """
    Run Atlas $vectorSearch on the application's embedding, then
    rerank results with voyage rerank-2.

    Args:
        application: Application dict (must have _id and narrative)

    Returns:
        Updated application dict with `similar_applications` list of IDs
        and `similar_applications_detail` list of dicts for use by the
        credit analysis activity.
    """
    app_id = application["_id"]
    activity.logger.info(f"[{app_id}] Running Atlas Vector Search")

    # ------------------------------------------------------------------
    # Step 1: Fetch the stored embedding from MongoDB
    # We stored it there (not in the workflow payload) to keep messages small.
    # ------------------------------------------------------------------
    collection = await get_collection("loan_applications")
    doc = await collection.find_one({"_id": app_id}, {"embedding": 1})
    if not doc or "embedding" not in doc:
        raise RuntimeError(f"No embedding found for application {app_id}")
    embedding = doc["embedding"]

    # ------------------------------------------------------------------
    # Step 2: Atlas Vector Search via $vectorSearch aggregation
    # Finds the top VECTOR_SEARCH_CANDIDATES most similar historical loans.
    # The index name "loan_embedding_index" was created by setup/create_vector_index.py.
    # We exclude the current application from results with a pre-filter.
    # ------------------------------------------------------------------
    pipeline = [
        {
            "$vectorSearch": {
                "index": "loan_embedding_index",
                "path": "embedding",
                "queryVector": embedding,
                "numCandidates": VECTOR_SEARCH_CANDIDATES,
                "limit": VECTOR_SEARCH_CANDIDATES,
                "filter": {
                    "_id": {"$ne": app_id},
                    "decision.outcome": {"$exists": True},  # Only decided loans
                },
            }
        },
        {
            "$project": {
                "_id": 1,
                "narrative": 1,
                "borrower": 1,
                "loan": 1,
                "ai_analysis": 1,
                "decision": 1,
                "score": {"$meta": "vectorSearchScore"},
            }
        },
    ]

    candidates = await collection.aggregate(pipeline).to_list(length=VECTOR_SEARCH_CANDIDATES)
    activity.logger.info(
        f"[{app_id}] Found {len(candidates)} similar loans via Atlas Vector Search"
    )

    if not candidates:
        activity.logger.warning(f"[{app_id}] No similar applications found — skipping rerank")
        similar_ids = []
        similar_detail = []
    else:
        # ------------------------------------------------------------------
        # Step 3: Rerank with Voyage rerank-2
        # Vector search scores similarity; reranking scores relevance.
        # This improves the quality of context passed to Claude.
        # ------------------------------------------------------------------
        activity.logger.info(f"[{app_id}] Reranking results via voyage rerank-2")
        voyage = voyageai.AsyncClient(api_key=os.environ["VOYAGE_API_KEY"])

        candidate_texts = [
            _format_candidate_for_reranking(c) for c in candidates
        ]

        rerank_result = await voyage.rerank(
            query=application.get("narrative", ""),
            documents=candidate_texts,
            model="rerank-2",
            top_k=TOP_K,
        )

        # Map reranked indices back to original candidates
        reranked = [candidates[r.index] for r in rerank_result.results]
        activity.logger.info(
            f"[{app_id}] Reranked results via rerank-2 — keeping top {len(reranked)}"
        )

        similar_ids = [str(c["_id"]) for c in reranked]
        similar_detail = [_summarize_candidate(c) for c in reranked]

    # ------------------------------------------------------------------
    # Step 4: Persist retrieved context to MongoDB
    # ------------------------------------------------------------------
    await collection.update_one(
        {"_id": app_id},
        {
            "$set": {
                "status": "retrieval_complete",
                "similar_applications": similar_ids,
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )

    activity.logger.info(f"[{app_id}] Similar application IDs stored in MongoDB Atlas")

    return {
        **application,
        "similar_applications": similar_ids,
        "similar_applications_detail": similar_detail,
    }


def _format_candidate_for_reranking(candidate: dict) -> str:
    """Build a plain-text summary of a historical loan for Voyage reranking."""
    b = candidate.get("borrower", {})
    l = candidate.get("loan", {})
    d = candidate.get("decision", {})
    return (
        f"Loan of ${l.get('amount', 0):,} for {l.get('purpose', 'unknown')}. "
        f"Income: ${b.get('annual_income', 0):,}, "
        f"credit score: {b.get('credit_score', 'N/A')}, "
        f"employment: {b.get('employment_status', 'N/A')}. "
        f"Outcome: {d.get('outcome', 'unknown')}."
    )


def _summarize_candidate(candidate: dict) -> dict:
    """Extract the fields needed by the credit analysis prompt."""
    return {
        "id": str(candidate["_id"]),
        "narrative": candidate.get("narrative", ""),
        "borrower": candidate.get("borrower", {}),
        "loan": candidate.get("loan", {}),
        "ai_analysis": candidate.get("ai_analysis", {}),
        "decision": candidate.get("decision", {}),
        "vector_score": candidate.get("score", 0.0),
    }
