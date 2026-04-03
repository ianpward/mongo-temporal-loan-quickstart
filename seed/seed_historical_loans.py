"""
Seed Historical Loan Decisions

Populates MongoDB Atlas with realistic historical loan applications
that already have decisions. These are used by Atlas Vector Search
to find similar cases for new applications.

Each loan is embedded via Voyage AI so the vector index can retrieve them.

Usage:
    python seed/seed_historical_loans.py

This is safe to run multiple times — it uses upsert so no duplicates
will be created.
"""

import asyncio
import os
from datetime import datetime, timezone, timedelta
import random

import voyageai
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

# ---------------------------------------------------------------------------
# Historical loan data — covers a range of profiles and outcomes
# ---------------------------------------------------------------------------

HISTORICAL_LOANS = [
    {
        "_id": "hist-loan-001",
        "borrower": {"name": "Alice Chen", "annual_income": 110_000, "credit_score": 760, "employment_status": "employed"},
        "loan": {"amount": 30_000, "purpose": "home renovation", "term_months": 60},
        "decision": {"outcome": "approved", "decided_by": "officer_12"},
        "notes": "Strong credit profile, stable employment, purpose aligns with collateral improvement",
    },
    {
        "_id": "hist-loan-002",
        "borrower": {"name": "Bob Martinez", "annual_income": 65_000, "credit_score": 640, "employment_status": "employed"},
        "loan": {"amount": 20_000, "purpose": "debt consolidation", "term_months": 48},
        "decision": {"outcome": "approved", "decided_by": "officer_07"},
        "notes": "Moderate risk; consolidation reduces overall exposure",
    },
    {
        "_id": "hist-loan-003",
        "borrower": {"name": "Carol White", "annual_income": 45_000, "credit_score": 580, "employment_status": "part-time"},
        "loan": {"amount": 15_000, "purpose": "medical expenses", "term_months": 36},
        "decision": {"outcome": "denied", "decided_by": "officer_12"},
        "notes": "Credit score below threshold; unstable income insufficient for loan size",
    },
    {
        "_id": "hist-loan-004",
        "borrower": {"name": "David Kim", "annual_income": 140_000, "credit_score": 810, "employment_status": "employed"},
        "loan": {"amount": 50_000, "purpose": "business expansion", "term_months": 84},
        "decision": {"outcome": "approved", "decided_by": "officer_19"},
        "notes": "Excellent credit, high income, established business track record",
    },
    {
        "_id": "hist-loan-005",
        "borrower": {"name": "Eva Patel", "annual_income": 78_000, "credit_score": 700, "employment_status": "employed"},
        "loan": {"amount": 22_000, "purpose": "education", "term_months": 60},
        "decision": {"outcome": "approved", "decided_by": "officer_07"},
        "notes": "Education loans carry lower default risk; income supports repayment",
    },
    {
        "_id": "hist-loan-006",
        "borrower": {"name": "Frank Rivera", "annual_income": 38_000, "credit_score": 550, "employment_status": "self-employed"},
        "loan": {"amount": 25_000, "purpose": "vacation", "term_months": 36},
        "decision": {"outcome": "denied", "decided_by": "officer_19"},
        "notes": "High DTI, non-essential purpose, low credit score, variable income",
    },
    {
        "_id": "hist-loan-007",
        "borrower": {"name": "Grace Lee", "annual_income": 92_000, "credit_score": 735, "employment_status": "employed"},
        "loan": {"amount": 35_000, "purpose": "home renovation", "term_months": 72},
        "decision": {"outcome": "approved", "decided_by": "officer_12"},
        "notes": "Good profile; renovation improves collateral value",
    },
    {
        "_id": "hist-loan-008",
        "borrower": {"name": "Henry Nguyen", "annual_income": 55_000, "credit_score": 660, "employment_status": "employed"},
        "loan": {"amount": 10_000, "purpose": "vehicle purchase", "term_months": 48},
        "decision": {"outcome": "approved", "decided_by": "officer_07"},
        "notes": "Secured loan with collateral; acceptable risk",
    },
    {
        "_id": "hist-loan-009",
        "borrower": {"name": "Iris Johnson", "annual_income": 30_000, "credit_score": 590, "employment_status": "unemployed"},
        "loan": {"amount": 18_000, "purpose": "debt consolidation", "term_months": 60},
        "decision": {"outcome": "denied", "decided_by": "officer_19"},
        "notes": "No current income source; unable to service new debt",
    },
    {
        "_id": "hist-loan-010",
        "borrower": {"name": "James Okafor", "annual_income": 125_000, "credit_score": 780, "employment_status": "employed"},
        "loan": {"amount": 40_000, "purpose": "home renovation", "term_months": 60},
        "decision": {"outcome": "approved", "decided_by": "officer_12"},
        "notes": "Strong financials across all metrics",
    },
    {
        "_id": "hist-loan-011",
        "borrower": {"name": "Karen Singh", "annual_income": 72_000, "credit_score": 690, "employment_status": "employed"},
        "loan": {"amount": 28_000, "purpose": "medical expenses", "term_months": 48},
        "decision": {"outcome": "approved", "decided_by": "officer_07"},
        "notes": "Medical hardship; good history despite mid-range credit",
    },
    {
        "_id": "hist-loan-012",
        "borrower": {"name": "Leo Tran", "annual_income": 48_000, "credit_score": 610, "employment_status": "part-time"},
        "loan": {"amount": 30_000, "purpose": "business expansion", "term_months": 60},
        "decision": {"outcome": "denied", "decided_by": "officer_19"},
        "notes": "Income volatility; amount too high relative to income; weak credit",
    },
    {
        "_id": "hist-loan-013",
        "borrower": {"name": "Maria Gonzalez", "annual_income": 88_000, "credit_score": 745, "employment_status": "employed"},
        "loan": {"amount": 20_000, "purpose": "debt consolidation", "term_months": 36},
        "decision": {"outcome": "approved", "decided_by": "officer_12"},
        "notes": "Consolidation reduces DTI; strong employment history",
    },
    {
        "_id": "hist-loan-014",
        "borrower": {"name": "Nathan Brown", "annual_income": 62_000, "credit_score": 670, "employment_status": "self-employed"},
        "loan": {"amount": 15_000, "purpose": "education", "term_months": 48},
        "decision": {"outcome": "approved", "decided_by": "officer_07"},
        "notes": "Self-employment verified; education purpose; manageable amount",
    },
    {
        "_id": "hist-loan-015",
        "borrower": {"name": "Olivia Park", "annual_income": 105_000, "credit_score": 795, "employment_status": "employed"},
        "loan": {"amount": 45_000, "purpose": "vehicle purchase", "term_months": 72},
        "decision": {"outcome": "approved", "decided_by": "officer_19"},
        "notes": "Excellent profile; auto loan with collateral",
    },
]


def build_narrative(loan: dict) -> str:
    """Build text narrative for embedding."""
    b = loan["borrower"]
    l = loan["loan"]
    d = loan["decision"]
    return (
        f"Loan application for {b['name']}. "
        f"Requesting ${l['amount']:,} over {l['term_months']} months "
        f"for: {l['purpose']}. "
        f"Borrower annual income: ${b['annual_income']:,}, "
        f"credit score: {b['credit_score']}, "
        f"employment status: {b['employment_status']}. "
        f"Outcome: {d['outcome']}. "
        f"Notes: {loan.get('notes', '')}"
    )


async def main() -> None:
    uri = os.environ["MONGODB_URI"]
    db_name = os.environ.get("MONGODB_DATABASE", "loan_processing")

    print(f"Connecting to MongoDB Atlas ({db_name})...")
    client = AsyncIOMotorClient(uri)
    collection = client[db_name]["loan_applications"]

    print(f"Generating embeddings for {len(HISTORICAL_LOANS)} historical loans via voyage-4-large...")
    voyage = voyageai.AsyncClient(api_key=os.environ["VOYAGE_API_KEY"])

    narratives = [build_narrative(loan) for loan in HISTORICAL_LOANS]

    # Embed all narratives in a single batch call
    result = await voyage.embed(
        texts=narratives,
        model="voyage-4-large",
        input_type="document",
    )

    print(f"Embeddings generated ({len(result.embeddings[0])} dimensions each)")
    print(f"Upserting {len(HISTORICAL_LOANS)} loans into Atlas...")

    base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for i, (loan, embedding, narrative) in enumerate(
        zip(HISTORICAL_LOANS, result.embeddings, narratives)
    ):
        submitted_at = base_date + timedelta(days=i * 7)
        decided_at = submitted_at + timedelta(days=random.randint(1, 5))

        doc = {
            **loan,
            "workflow_id": f"seed-workflow-{loan['_id']}",
            "status": "decided",
            "submitted_at": submitted_at,
            "narrative": narrative,
            "embedding": embedding,
            "similar_applications": [],
            "ai_analysis": {
                "risk_score": round(random.uniform(0.1, 0.9), 4),
                "recommendation": loan["decision"]["outcome"].replace("approved", "approve").replace("denied", "deny"),
                "rationale": f"Historical seed record. {loan.get('notes', '')}",
            },
            "decision": {
                **loan["decision"],
                "decided_at": decided_at,
            },
            "updated_at": decided_at,
        }

        await collection.update_one(
            {"_id": loan["_id"]},
            {"$set": doc},
            upsert=True,
        )
        print(f"  ✓ {loan['_id']} — {loan['borrower']['name']} — {loan['decision']['outcome']}")

    print(f"\nSeed complete: {len(HISTORICAL_LOANS)} historical loans in Atlas ✓")
    print("You can now run the worker and submit applications.")


if __name__ == "__main__":
    asyncio.run(main())
