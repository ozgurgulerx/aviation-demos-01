import os
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List

from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from dotenv import load_dotenv
from openai import AzureOpenAI

ROOT = Path(__file__).resolve().parents[1]

# Keep existing search/runtime config from repository root, but force OpenAI auth/path
# to the tenant-aligned Foundry endpoint for live integration checks.
load_dotenv(ROOT / ".env", override=False)
os.environ.setdefault(
    "AZURE_OPENAI_ENDPOINT",
    "https://ai-eastus2hubozguler527669401205.cognitiveservices.azure.com/",
)
os.environ.setdefault("AZURE_OPENAI_DEPLOYMENT_NAME", "aviation-chat-gpt5-mini")
os.environ.setdefault("AZURE_TEXT_EMBEDDING_DEPLOYMENT_NAME", "text-embedding-3-small")
os.environ["AZURE_OPENAI_API_KEY"] = ""

sys.path.insert(0, str(ROOT / "src"))
from af_runtime import AgentFrameworkRuntime  # noqa: E402


CASES: List[Dict[str, Any]] = [
    {
        "name": "sql_metrics",
        "query": "Top 5 facilities by ASRS report count and average damage score.",
        "query_profile": "pilot-brief",
        "required_sources": ["SQL"],
    },
    {
        "name": "regulatory_brief",
        "query": "Summarize the most relevant NOTAM or airworthiness themes for Istanbul operations this week.",
        "query_profile": "compliance",
        "required_sources": ["VECTOR_REG"],
    },
    {
        "name": "kql_live_window",
        "query": "Report live hazard indicators for IST departures in the last 30 minutes.",
        "query_profile": "ops-live",
        "required_sources": ["KQL"],
        "freshness_sla_minutes": 30,
    },
    {
        "name": "graph_dependencies",
        "query": "Explain dependency paths that can propagate departure disruption from IST to downstream stations.",
        "query_profile": "ops-live",
        "required_sources": ["GRAPH"],
    },
    {
        "name": "full_agentic_mix",
        "query": "Build an operations risk brief for IST using SQL metrics, live hazards, dependency graph, regulatory notes, and NoSQL snapshots.",
        "query_profile": "ops-live",
        "required_sources": ["SQL", "KQL", "GRAPH", "VECTOR_REG", "NOSQL"],
        "freshness_sla_minutes": 60,
        "ask_recommendation": True,
    },
]


class AgenticLiveQueriesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._assert_openai_live_ready()
        cls.runtime = AgentFrameworkRuntime()

    @classmethod
    def _assert_openai_live_ready(cls) -> None:
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").strip()
        deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "").strip()
        if not endpoint or not deployment:
            raise unittest.SkipTest("Missing AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_DEPLOYMENT_NAME for live tests.")

        try:
            token_provider = get_bearer_token_provider(
                DefaultAzureCredential(),
                "https://cognitiveservices.azure.com/.default",
            )
            client = AzureOpenAI(
                azure_endpoint=endpoint,
                azure_ad_token_provider=token_provider,
                api_version="2024-06-01",
                timeout=30,
                max_retries=1,
            )
            client.chat.completions.create(
                model=deployment,
                messages=[{"role": "user", "content": "Reply with READY"}],
            )
        except Exception as exc:
            raise unittest.SkipTest(f"Azure OpenAI live check failed: {exc}") from exc

    def _run_case(self, case: Dict[str, Any]) -> Dict[str, Any]:
        events = list(
            self.runtime.run_stream(
                query=case["query"],
                retrieval_mode="code-rag",
                query_profile=case.get("query_profile", "pilot-brief"),
                required_sources=case.get("required_sources", []),
                freshness_sla_minutes=case.get("freshness_sla_minutes"),
                explain_retrieval=True,
                risk_mode="standard",
                ask_recommendation=bool(case.get("ask_recommendation", False)),
            )
        )

        answer = "".join(
            str(event.get("content", ""))
            for event in events
            if event.get("type") == "agent_update" and event.get("content")
        ).strip()
        retrieval_plan = next((event.get("plan", {}) for event in events if event.get("type") == "retrieval_plan"), {})
        source_starts = {
            str(event.get("source", "")).upper()
            for event in events
            if event.get("type") == "source_call_start" and event.get("source")
        }
        source_dones = {
            str(event.get("source", "")).upper()
            for event in events
            if event.get("type") == "source_call_done" and event.get("source")
        }
        done_event = next((event for event in events if event.get("type") == "agent_done"), {})
        agent_errors = [event for event in events if event.get("type") == "agent_error"]

        return {
            "events": events,
            "answer": answer,
            "retrieval_plan": retrieval_plan,
            "source_starts": source_starts,
            "source_dones": source_dones,
            "done_event": done_event,
            "agent_errors": agent_errors,
        }

    def _assert_case(self, case: Dict[str, Any]) -> None:
        result = self._run_case(case)
        answer = result["answer"]
        retrieval_plan = result["retrieval_plan"]
        done_event = result["done_event"]

        self.assertTrue(answer, f"{case['name']}: answer was empty")
        self.assertNotIn("Unable to synthesize with model right now", answer, f"{case['name']}: LLM synthesis failed")
        self.assertFalse(result["agent_errors"], f"{case['name']}: agent_error events present")
        self.assertTrue(done_event, f"{case['name']}: missing agent_done")
        self.assertTrue(retrieval_plan.get("steps"), f"{case['name']}: missing retrieval plan steps")

        for source in case.get("required_sources", []):
            expected = str(source).upper()
            self.assertIn(expected, result["source_starts"], f"{case['name']}: missing source_call_start for {expected}")
            self.assertIn(expected, result["source_dones"], f"{case['name']}: missing source_call_done for {expected}")

        answer_preview = answer[:260].replace("\n", " ")
        route = done_event.get("route", "UNKNOWN")
        print(f"[{case['name']}] route={route} answer={answer_preview}")

    def test_live_query_01_sql_metrics(self):
        self._assert_case(CASES[0])

    def test_live_query_02_regulatory_brief(self):
        self._assert_case(CASES[1])

    def test_live_query_03_kql_live_window(self):
        self._assert_case(CASES[2])

    def test_live_query_04_graph_dependencies(self):
        self._assert_case(CASES[3])

    def test_live_query_05_full_agentic_mix(self):
        self._assert_case(CASES[4])


if __name__ == "__main__":
    unittest.main()
