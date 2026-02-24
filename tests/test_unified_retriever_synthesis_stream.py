import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from unified_retriever import UnifiedRetriever  # noqa: E402


class _FakeCompletions:
    def __init__(self, chunks):
        self._chunks = chunks

    def create(self, **kwargs):
        return iter(self._chunks)


class _FakeDelta:
    def __init__(self, content=None, refusal=None):
        self.content = content
        self.refusal = refusal


class _FakeChoice:
    def __init__(self, delta=None, text=None, content=None, refusal=None):
        self.delta = delta
        self.text = text
        self.content = content
        self.refusal = refusal


class _FakeChunk:
    def __init__(self, delta=None, choice_text=None, choice_content=None, output_text=None):
        self.choices = [_FakeChoice(delta=delta, text=choice_text, content=choice_content)]
        self.output_text = output_text


class UnifiedRetrieverSynthesisStreamTests(unittest.TestCase):
    def _build_retriever(self, chunks):
        retriever = object.__new__(UnifiedRetriever)
        retriever.llm_deployment = "test-model"
        retriever.llm = types.SimpleNamespace(
            chat=types.SimpleNamespace(
                completions=_FakeCompletions(chunks),
            )
        )
        return retriever

    def test_structured_delta_content_is_streamed_as_text(self):
        retriever = self._build_retriever(
            [
                _FakeChunk(_FakeDelta(content=[{"text": "Alpha "}, {"text": "Bravo"}])),
                _FakeChunk(_FakeDelta(content=" Charlie")),
            ]
        )

        events = list(retriever._synthesize_answer_stream("query", {"x": 1}, "HYBRID"))
        chunks = [
            str(event.get("content", ""))
            for event in events
            if event.get("type") == "agent_update"
        ]

        self.assertEqual("".join(chunks), "Alpha Bravo Charlie")

    def test_refusal_without_text_emits_terminal_agent_error(self):
        retriever = self._build_retriever(
            [_FakeChunk(_FakeDelta(content=None, refusal="I can't comply with this request."))]
        )

        events = list(retriever._synthesize_answer_stream("query", {"x": 1}, "HYBRID"))
        updates = [event for event in events if event.get("type") == "agent_update"]
        errors = [event for event in events if event.get("type") == "agent_error"]

        self.assertFalse(updates)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].get("error_code"), "llm_refusal")
        self.assertIn("comply", str(errors[0].get("message", "")).lower())

    def test_choice_text_without_delta_is_streamed(self):
        retriever = self._build_retriever(
            [
                _FakeChunk(delta=None, choice_text="Alpha "),
                _FakeChunk(delta=None, choice_text="Bravo"),
            ]
        )

        events = list(retriever._synthesize_answer_stream("query", {"x": 1}, "HYBRID"))
        chunks = [
            str(event.get("content", ""))
            for event in events
            if event.get("type") == "agent_update"
        ]
        self.assertEqual("".join(chunks), "Alpha Bravo")

    def test_chunk_output_text_without_delta_is_streamed(self):
        retriever = self._build_retriever(
            [
                _FakeChunk(delta=None, output_text="Gamma "),
                _FakeChunk(delta=None, output_text="Delta"),
            ]
        )

        events = list(retriever._synthesize_answer_stream("query", {"x": 1}, "HYBRID"))
        chunks = [
            str(event.get("content", ""))
            for event in events
            if event.get("type") == "agent_update"
        ]
        self.assertEqual("".join(chunks), "Gamma Delta")


if __name__ == "__main__":
    unittest.main()
