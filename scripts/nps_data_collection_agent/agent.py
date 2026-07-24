"""
scripts/marketing_data_collection_agent/agent.py

Marketing Data Collection Agent — Custom Google ADK Agent
Pulls marketing ad spend & performance data across Google Ads, LinkedIn Ads,
Microsoft Ads, and Capterra from BigQuery, and fetches live Salesforce Leads
and Opportunities via the custom Salesforce MCP server.

Calculates precomputed efficiency and funnel metrics (CTR, CPC, CPL, Cost Per Opp,
Lead-to-Opp Conversion Rate) overall and monthwise.

Input  (session state): optional `lookback_days` (defaults to 365 days).
Output (session state): `marketing_payload` containing overall campaign summary,
                       monthwise cross-channel trends, and raw lead context.
"""

import asyncio
import json
import os
from datetime import datetime
from urllib.parse import urlsplit

from google.adk.agents import BaseAgent
from google.adk.events import Event, EventActions
from google.adk.runners import InMemoryRunner
from google.auth.transport import requests as google_auth_requests
from google.cloud import bigquery
from google.oauth2 import id_token
from mcp import ClientSession
from mcp.client.sse import sse_client

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
GCP_PROJECT_ID = "atgeir-moae-dev"
DATASET_ID = "marketing_agent"

TABLE_GOOGLE_ADS = f"{GCP_PROJECT_ID}.{DATASET_ID}.google_ads"
TABLE_LINKEDIN_ADS = f"{GCP_PROJECT_ID}.{DATASET_ID}.linkedin_ads"
TABLE_CAPTERRA_ADS = f"{GCP_PROJECT_ID}.{DATASET_ID}.capterra"
TABLE_MICROSOFT_ADS = f"{GCP_PROJECT_ID}.{DATASET_ID}.microsoft_ads"

DEFAULT_LOOKBACK_DAYS = 365

# Salesforce Leads & Opportunities come via custom MCP Cloud Run Endpoint
MCP_SALESFORCE_SERVER_URL = os.environ.get(
    "MCP_SALESFORCE_SERVER_URL",  "https://salesforce-mcp-server-v3-621913909275.us-central1.run.app/sse"
)

# Extract base URL for IAM identity token validation
_mcp_url_parts = urlsplit(MCP_SALESFORCE_SERVER_URL)
MCP_SALESFORCE_SERVER_BASE_URL = f"{_mcp_url_parts.scheme}://{_mcp_url_parts.netloc}"

MCP_CONCURRENCY_LIMIT = 5
_mcp_semaphore = asyncio.Semaphore(MCP_CONCURRENCY_LIMIT)


# ─────────────────────────────────────────────
# 0) MCP CLIENT — Salesforce Live Calls
# ─────────────────────────────────────────────
#async def _get_gcp_identity_token(audience: str) -> str:
 #   """Fetch GCP identity token for Cloud Run MCP server auth."""
  # loop = asyncio.get_event_loop()
  # return await loop.run_in_executor(
  #    None, id_token.fetch_id_token, google_auth_requests.Request(), audience
  # )

import subprocess

async def _get_gcp_identity_token(audience: str) -> str:
    """Fetch GCP identity token locally via gcloud, falling back to GCP environment."""
    def _fetch():
        # 1. Try local gcloud CLI first (Works for local human user accounts)
        try:
            cmd = 'gcloud auth print-identity-token'
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            token = result.stdout.strip()
            if token and result.returncode == 0:
                return token
        except Exception as e:
            print(f"[Debug] gcloud print-identity-token failed: {e}")

        # 2. Fallback to Google Auth library (for deployed Cloud Run environments)
        auth_req = google_auth_requests.Request()
        return id_token.fetch_id_token(auth_req, audience)

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch)

#import subprocess

#async def _get_gcp_identity_token(audience: str) -> str:
  #  """Fetch GCP identity token using gcloud CLI locally, falling back to id_token on Cloud."""
 #   def _fetch():
  #      try:
            # 1. Try local gcloud CLI first (Works on Windows/Mac/Linux with ADC)
   #         cmd = f'gcloud auth print-identity-token --audiences="{audience}"'
    #       token = result.stdout.strip()
     #       if token and not result.returncode:
      #          return token
      #  except Exception:
       #     pass

        # 2. Fallback to Google Auth library (Works when deployed to Cloud Run / GCP)
       # auth_req = google_auth_requests.Request()
       # return id_token.fetch_id_token(auth_req, audience)

   # loop = asyncio.get_event_loop()
    #return await loop.run_in_executor(None, _fetch)

async def _call_mcp_tool(tool_name: str, arguments: dict) -> dict:
    """Opens SSE session to Salesforce MCP server and executes tool call."""
    identity_token = await _get_gcp_identity_token(MCP_SALESFORCE_SERVER_BASE_URL)
    async with sse_client(
            MCP_SALESFORCE_SERVER_URL,
            headers={"Authorization": f"Bearer {identity_token}"}
    ) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments)
            if result.isError:
                raise RuntimeError(f"MCP tool '{tool_name}' returned error: {result.content}")
            return json.loads(result.content[0].text)


async def _fetch_sf_leads_mcp(lookback_days: int) -> list[dict]:
    """Fetch Salesforce leads created within lookback window via MCP."""
    async with _mcp_semaphore:
        try:
            mcp_result = await _call_mcp_tool(
                "get_leads", {"lookback_days": lookback_days}
            )
            return mcp_result.get("leads", [])
        except Exception as e:
            print(f"[Warning] MCP Lead Fetch failed: {e}. Falling back to empty list.")
            return []


async def _fetch_sf_opportunities_mcp(lookback_days: int) -> list[dict]:
    """Fetch Salesforce opportunities created/converted via MCP."""
    async with _mcp_semaphore:
        try:
            mcp_result = await _call_mcp_tool(
                "get_opportunities", {"lookback_days": lookback_days}
            )
            return mcp_result.get("opportunities", [])
        except Exception as e:
            print(f"[Warning] MCP Opportunity Fetch failed: {e}. Falling back to empty list.")
            return []


# ─────────────────────────────────────────────
# 1) BIGQUERY FETCH FUNCTIONS (Ad Platforms)
# ─────────────────────────────────────────────

def _fetch_google_ads_sync(lookback_days: int) -> list[dict]:
    """Fetches key performance metrics from Google Ads."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT
            PARSE_DATE('%Y-%m-%d', CAST(report_date AS STRING)) AS report_date,
            campaign_id,
            campaign_name,
            clicks,
            impressions,
            spend,
            conversions AS platform_conversions
        FROM `{TABLE_GOOGLE_ADS}`
       
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", lookback_days)]
    )
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


def _fetch_linkedin_ads_sync(lookback_days: int) -> list[dict]:
    """Fetches performance metrics from LinkedIn Ads."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT
            PARSE_DATE('%Y-%m-%d', CAST(date AS STRING)) AS report_date,
            campaign_id,
            campaign_name,
            target_audience_segment,
            clicks,
            impressions,
            cost AS spend,
            total_conversions AS platform_conversions
        FROM `{TABLE_LINKEDIN_ADS}`
        WHERE PARSE_DATE('%Y-%m-%d', CAST(date AS STRING)) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", lookback_days)]
    )
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


def _fetch_capterra_ads_sync(lookback_days: int) -> list[dict]:
    """Extracts cost and click performance data from Capterra."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT
            PARSE_DATE('%Y-%m-%d', CAST(date AS STRING)) AS report_date,
            campaign_id,
            campaign_name,
            clicks,
            0 AS impressions,
            cost AS spend,
            0 AS platform_conversions
        FROM `{TABLE_CAPTERRA_ADS}`
        WHERE PARSE_DATE('%Y-%m-%d', CAST(date AS STRING)) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", lookback_days)]
    )
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


def _fetch_microsoft_ads_sync(lookback_days: int) -> list[dict]:
    """Fetches PPC data from Microsoft Ads."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT
            PARSE_DATE('%Y-%m-%d', CAST(date AS STRING)) AS report_date,
            campaign_id,
            campaign_name,
            clicks,
            impressions,
            spend,
            conversions AS platform_conversions
        FROM `{TABLE_MICROSOFT_ADS}`
        WHERE PARSE_DATE('%Y-%m-%d', CAST(date AS STRING)) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", lookback_days)]
    )
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


# ─────────────────────────────────────────────
# ASYNC WRAPPER
# ─────────────────────────────────────────────

async def _run(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, fn, *args)


# ─────────────────────────────────────────────
# PRECOMPUTATION & METRIC AGGREGATION
# ─────────────────────────────────────────────

def _safe_div(num: float, den: float, multiplier: float = 1.0) -> float | None:
    if not den or den == 0:
        return None
    return round((num / den) * multiplier, 2)


def process_marketing_and_sf_data(
        google_rows: list[dict],
        linkedin_rows: list[dict],
        microsoft_rows: list[dict],
        capterra_rows: list[dict],
        sf_leads: list[dict],
        sf_opps: list[dict],
) -> dict:
    """
    Combines BQ marketing records with SF Leads and Opportunities.
    Calculates overall and monthwise precomputed marketing performance metrics.
    """
    # 1. Unify Ad Rows
    all_ad_rows = []
    for r in google_rows:
        all_ad_rows.append({**r, "channel": "Google Ads"})
    for r in linkedin_rows:
        all_ad_rows.append({**r, "channel": "LinkedIn Ads"})
    for r in microsoft_rows:
        all_ad_rows.append({**r, "channel": "Microsoft Ads"})
    for r in capterra_rows:
        all_ad_rows.append({**r, "channel": "Capterra"})

    # 2. Summarize Salesforce Leads by LeadSource and Month
    sf_summary_by_source = {}
    sf_monthly_by_source = {}

    for lead in sf_leads:
        source = (lead.get("LeadSource") or "Unknown").strip().lower()
        has_opp = bool(lead.get("converted_opportunity_id__c"))

        # Aggregate overall by source
        if source not in sf_summary_by_source:
            sf_summary_by_source[source] = {"leads": 0, "opps": 0}
        sf_summary_by_source[source]["leads"] += 1
        if has_opp:
            sf_summary_by_source[source]["opps"] += 1

        # Aggregate monthwise
        created_date_str = lead.get("created_date__c")
        if created_date_str:
            ym = created_date_str[:7]
            key = (ym, source)
            if key not in sf_monthly_by_source:
                sf_monthly_by_source[key] = {"leads": 0, "opps": 0}
            sf_monthly_by_source[key]["leads"] += 1
            if has_opp:
                sf_monthly_by_source[key]["opps"] += 1

    # 3. Build Query 1 equivalent: Total Performance Summary Overall
    campaign_aggregates = {}
    for ad in all_ad_rows:
        key = (ad["channel"], ad["campaign_name"])
        if key not in campaign_aggregates:
            campaign_aggregates[key] = {
                "spend": 0.0, "clicks": 0, "impressions": 0, "conversions": 0
            }
        campaign_aggregates[key]["spend"] += float(ad.get("spend") or 0)
        campaign_aggregates[key]["clicks"] += int(ad.get("clicks") or 0)
        campaign_aggregates[key]["impressions"] += int(ad.get("impressions") or 0)
        campaign_aggregates[key]["conversions"] += int(ad.get("platform_conversions") or 0)

    overall_performance = []
    for (channel, campaign_name), ad_metrics in campaign_aggregates.items():
        lookup_key = campaign_name.strip().lower()
        sf_data = sf_summary_by_source.get(lookup_key, {"leads": 0, "opps": 0})

        spend = ad_metrics["spend"]
        clicks = ad_metrics["clicks"]
        impressions = ad_metrics["impressions"]
        leads = sf_data["leads"]
        opps = sf_data["opps"]

        overall_performance.append({
            "channel": channel,
            "campaign_name": campaign_name,
            "total_spend": round(spend, 2),
            "total_clicks": clicks,
            "total_impressions": impressions,
            "platform_conversions": ad_metrics["conversions"],
            "total_sf_leads": leads,
            "total_sf_opportunities": opps,
            "ctr_percent": _safe_div(clicks, impressions, 100.0),
            "cpc": _safe_div(spend, clicks),
            "cpl": _safe_div(spend, leads),
            "lead_to_opp_conversion_rate": _safe_div(opps, leads, 100.0),
            "cost_per_opportunity": _safe_div(spend, opps)
        })

    # 4. Build Query 2 equivalent: Monthwise Performance Trend
    monthly_aggregates = {}
    for ad in all_ad_rows:
        date_obj = ad.get("report_date")
        ym = date_obj.strftime("%Y-%m") if hasattr(date_obj, "strftime") else str(date_obj)[:7]
        key = (ym, ad["channel"], ad["campaign_name"])

        if key not in monthly_aggregates:
            monthly_aggregates[key] = {
                "spend": 0.0, "clicks": 0, "impressions": 0, "conversions": 0
            }
        monthly_aggregates[key]["spend"] += float(ad.get("spend") or 0)
        monthly_aggregates[key]["clicks"] += int(ad.get("clicks") or 0)
        monthly_aggregates[key]["impressions"] += int(ad.get("impressions") or 0)
        monthly_aggregates[key]["conversions"] += int(ad.get("platform_conversions") or 0)

    monthly_performance = []
    for (ym, channel, campaign_name), ad_metrics in monthly_aggregates.items():
        lookup_key = (ym, campaign_name.strip().lower())
        sf_data = sf_monthly_by_source.get(lookup_key, {"leads": 0, "opps": 0})

        spend = ad_metrics["spend"]
        clicks = ad_metrics["clicks"]
        impressions = ad_metrics["impressions"]
        leads = sf_data["leads"]
        opps = sf_data["opps"]

        monthly_performance.append({
            "year_month": ym,
            "channel": channel,
            "campaign_name": campaign_name,
            "spend": round(spend, 2),
            "clicks": clicks,
            "impressions": impressions,
            "ad_conversions": ad_metrics["conversions"],
            "sf_leads": leads,
            "sf_opportunities": opps,
            "monthly_ctr_percent": _safe_div(clicks, impressions, 100.0),
            "monthly_cpc": _safe_div(spend, clicks),
            "monthly_cpl": _safe_div(spend, leads),
            "monthly_lead_to_opp_rate": _safe_div(opps, leads, 100.0)
        })

    return {
        "overall_performance": overall_performance,
        "monthly_performance": sorted(monthly_performance, key=lambda x: x["year_month"], reverse=True),
        "raw_leads_sample_count": len(sf_leads),
        "raw_opps_sample_count": len(sf_opps)
    }


# ─────────────────────────────────────────────
# CUSTOM ADK AGENT
# ─────────────────────────────────────────────

class MarketingDataCollectionAgent(BaseAgent):
    """
    Marketing Data Collection Agent.

    Reads advertising spend across Google, LinkedIn, Microsoft, and Capterra from BQ
    and fetches live Salesforce CRM Leads & Opportunities via MCP. Produces unified
    marketing efficiency calculations and emits `marketing_payload` into session state.
    """

    async def _run_async_impl(self, ctx):
        lookback_days = ctx.session.state.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
        print(f"\n[MarketingDataCollectionAgent] Starting run with lookback = {lookback_days} days")

        # Step 1: Fetch BigQuery Marketing Data in parallel via executors
        google_task = _run(_fetch_google_ads_sync, lookback_days)
        linkedin_task = _run(_fetch_linkedin_ads_sync, lookback_days)
        microsoft_task = _run(_fetch_microsoft_ads_sync, lookback_days)
        capterra_task = _run(_fetch_capterra_ads_sync, lookback_days)

        # Step 2: Fetch Salesforce Leads & Opps via MCP in parallel
        sf_leads_task = _fetch_sf_leads_mcp(lookback_days)
        sf_opps_task = _fetch_sf_opportunities_mcp(lookback_days)

        # Execute all tasks concurrently
        results = await asyncio.gather(
            google_task,
            linkedin_task,
            microsoft_task,
            capterra_task,
            sf_leads_task,
            sf_opps_task,
            return_exceptions=True
        )

        google_rows = results[0] if not isinstance(results[0], Exception) else []
        linkedin_rows = results[1] if not isinstance(results[1], Exception) else []
        microsoft_rows = results[2] if not isinstance(results[2], Exception) else []
        capterra_rows = results[3] if not isinstance(results[3], Exception) else []
        sf_leads = results[4] if not isinstance(results[4], Exception) else []
        sf_opps = results[5] if not isinstance(results[5], Exception) else []

        print(
            f"[MarketingDataCollectionAgent] Records Fetched -> "
            f"Google: {len(google_rows)}, LinkedIn: {len(linkedin_rows)}, "
            f"Microsoft: {len(microsoft_rows)}, Capterra: {len(capterra_rows)}, "
            f"SF Leads: {len(sf_leads)}, SF Opps: {len(sf_opps)}"
        )

        # Step 3: Compute aggregations and precalculated metrics
        marketing_payload = process_marketing_and_sf_data(
            google_rows=google_rows,
            linkedin_rows=linkedin_rows,
            microsoft_rows=microsoft_rows,
            capterra_rows=capterra_rows,
            sf_leads=sf_leads,
            sf_opps=sf_opps
        )

        print(
            f"\n── Calculated Overall Campaign Performance ({len(marketing_payload['overall_performance'])} items) ──")
        print(json.dumps(marketing_payload["overall_performance"], indent=2, default=str))

        # Step 4: Emit event state update
        yield Event(
            author=self.name,
            content=None,
            actions=EventActions(state_delta={"marketing_payload": marketing_payload}),
        )


marketing_data_collection_agent = MarketingDataCollectionAgent(name="marketing_data_collection_agent")


# ─────────────────────────────────────────────
# LOCAL TEST
# ─────────────────────────────────────────────

async def test():
    from google.genai import types

    # Mock identity token for local dev testing
#    global _get_gcp_identity_token

 #   async def _dummy_identity_token(audience: str) -> str:
  #      return "local-dev-dummy-token"

  #  _get_gcp_identity_token = _dummy_identity_token

    runner = InMemoryRunner(
        agent=MarketingDataCollectionAgent(name="MarketingDataCollectionAgent"),
        app_name="marketing_pipeline",
    )

    session_service = runner.session_service

    session = await session_service.create_session(
        app_name="marketing_pipeline",
        user_id="test_user",
        state={"lookback_days": 1025},
    )

    async for event in runner.run_async(
            user_id="test_user",
            session_id=session.id,
            new_message=types.Content(role="user", parts=[types.Part(text="start")]),
    ):
        print("\nEvent received from:", event.author)

    final = await session_service.get_session(
        app_name="marketing_pipeline", user_id="test_user", session_id=session.id,
    )

    payload = final.state.get("marketing_payload", {})

    print("\n── Final session state: marketing_payload (Overall Sample) ──")
    print(json.dumps(payload.get("overall_performance", []), indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(test())
