"""
Activity: run_credit_analysis

Uses Anthropic Claude to generate an underwriting summary and recommendation
for a loan application, informed by semantically similar historical decisions.

After this activity the application document in Atlas will have:
  - status: "analysis_complete"
  - ai_analysis.risk_score: float (0.0 = low risk, 1.0 = high risk)
  - ai_analysis.recommendation: "approve" | "deny" | "manual_review"
  - ai_analysis.rationale: str
"""

import json
import os
import re
from datetime import datetime, timezone

import anthropic
from temporalio import activity

from .db import get_collection

MODEL = "claude-sonnet-4-6"


def _make_anthropic_client() -> anthropic.AsyncAnthropic:
    """
    Build an Anthropic async client.

    TWO MODES — uncomment the one you want:

    1. Grove API gateway (current/default)
       Requires ANTHROPIC_API_KEY and optionally ANTHROPIC_BASE_URL in .env.
       The gateway expects an 'api-key' header instead of the SDK default 'x-api-key'.

    2. Direct Anthropic API
       Just needs ANTHROPIC_API_KEY. Comment out the gateway block and
       uncomment the two lines below to switch.
    """
    api_key = os.environ["ANTHROPIC_API_KEY"]

    # ------------------------------------------------------------------
    # MODE 1: Grove API gateway (active)
    # ------------------------------------------------------------------
    base_url = os.environ.get(
        "ANTHROPIC_BASE_URL",
        "https://grove-gateway-prod.azure-api.net/grove-foundry-prod/anthropic",
    )
    return anthropic.AsyncAnthropic(
        api_key=api_key,           # satisfies SDK validation
        base_url=base_url,
        default_headers={"api-key": api_key},  # header the gateway expects
    )

    # ------------------------------------------------------------------
    # MODE 2: Direct Anthropic API — uncomment to use, comment out MODE 1
    # ------------------------------------------------------------------
    # return anthropic.AsyncAnthropic(api_key=api_key)


@activity.defn
async def run_credit_analysis(application: dict) -> dict:
    """
    Call Claude to perform underwriting analysis.

    The prompt includes:
    - Borrower profile and loan request
    - AI recommendation and outcome of similar historical applications
    - Instructions to return structured JSON

    Args:
        application: Application dict with `similar_applications_detail`

    Returns:
        Updated application dict with `ai_analysis` containing
        risk_score, recommendation, and rationale.
    """
    app_id = application["_id"]
    activity.logger.info(f"[{app_id}] Running credit analysis via Claude ({MODEL})")

    similar = application.get("similar_applications_detail", [])

    prompt = _build_underwriting_prompt(application, similar)

    # ------------------------------------------------------------------
    # Call Claude via Grove API gateway
    # ------------------------------------------------------------------
    client = _make_anthropic_client()
    message = await client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text
    activity.logger.info(f"[{app_id}] Received analysis from Claude")

    # ------------------------------------------------------------------
    # Parse structured JSON from Claude's response
    # ------------------------------------------------------------------
    ai_analysis = _parse_analysis(raw)
    activity.logger.info(
        f"[{app_id}] Risk score {ai_analysis['risk_score']:.2f} — "
        f"recommendation: {ai_analysis['recommendation'].upper()}"
    )

    # ------------------------------------------------------------------
    # Persist to MongoDB
    # ------------------------------------------------------------------
    collection = await get_collection("loan_applications")
    await collection.update_one(
        {"_id": app_id},
        {
            "$set": {
                "status": "analysis_complete",
                "ai_analysis": ai_analysis,
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )

    activity.logger.info(f"[{app_id}] AI analysis stored in MongoDB Atlas")
    return {**application, "ai_analysis": ai_analysis}


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are an expert underwriting assistant at a consumer lending company.
Your role is to analyze loan applications and provide a data-driven credit assessment.

You must respond with a single JSON object — no markdown, no prose outside the JSON.
The object must have exactly these fields:
  {
    "risk_score": <float between 0.0 and 1.0, where 0.0 = very low risk>,
    "recommendation": <"approve" | "deny" | "manual_review">,
    "rationale": <2-4 sentence explanation of your reasoning>
  }

Guidelines:
- Credit score >= 700 and DTI < 36% are generally favorable
- Stable employment history reduces risk
- Loan purpose matters: home improvement and education carry lower risk than discretionary
- Similar past applications with the same profile and their outcomes should strongly inform your assessment
- Be conservative: when uncertain, recommend manual_review rather than approve/deny
"""


def _build_underwriting_prompt(application: dict, similar: list[dict]) -> str:
    borrower = application["borrower"]
    loan = application["loan"]

    # Estimate debt-to-income ratio if possible
    monthly_income = borrower["annual_income"] / 12
    estimated_monthly_payment = (loan["amount"] / loan["term_months"]) * 1.08  # rough APR estimate
    dti = estimated_monthly_payment / monthly_income if monthly_income > 0 else 0

    similar_section = ""
    if similar:
        cases = []
        for i, s in enumerate(similar, 1):
            b = s.get("borrower", {})
            l = s.get("loan", {})
            d = s.get("decision", {})
            a = s.get("ai_analysis", {})
            cases.append(
                f"  Case {i}: ${l.get('amount', 0):,} loan for {l.get('purpose', 'unknown')}, "
                f"income ${b.get('annual_income', 0):,}, credit score {b.get('credit_score', 'N/A')}, "
                f"employment {b.get('employment_status', 'N/A')}. "
                f"AI risk score: {a.get('risk_score', 'N/A')}. "
                f"Final outcome: {d.get('outcome', 'pending')}."
            )
        similar_section = "SIMILAR HISTORICAL APPLICATIONS:\n" + "\n".join(cases)
    else:
        similar_section = "SIMILAR HISTORICAL APPLICATIONS: None found."

    return f"""
LOAN APPLICATION:
  Applicant: {borrower['name']}
  Annual Income: ${borrower['annual_income']:,}
  Credit Score: {borrower['credit_score']}
  Employment Status: {borrower['employment_status']}
  Loan Amount: ${loan['amount']:,}
  Loan Purpose: {loan['purpose']}
  Term: {loan['term_months']} months
  Estimated Monthly Payment: ${estimated_monthly_payment:,.0f}
  Estimated DTI: {dti:.1%}

{similar_section}

Based on this information, provide your underwriting assessment as JSON.
""".strip()


def _parse_analysis(raw: str) -> dict:
    """Extract and validate the JSON block from Claude's response."""
    # Strip markdown code fences if present
    clean = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()

    try:
        data = json.loads(clean)
    except json.JSONDecodeError:
        # Fallback: try to find a JSON object in the response
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            raise ValueError(f"Could not parse JSON from Claude response: {raw[:200]}")
        data = json.loads(match.group())

    # Validate and clamp risk_score
    risk_score = float(data.get("risk_score", 0.5))
    risk_score = max(0.0, min(1.0, risk_score))

    recommendation = data.get("recommendation", "manual_review").lower()
    if recommendation not in {"approve", "deny", "manual_review"}:
        recommendation = "manual_review"

    return {
        "risk_score": round(risk_score, 4),
        "recommendation": recommendation,
        "rationale": str(data.get("rationale", "")),
    }
