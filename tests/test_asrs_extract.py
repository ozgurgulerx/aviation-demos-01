import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


def load_extract_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "01_extract_data.py"
    spec = importlib.util.spec_from_file_location("asrs_extract", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class AsrsExtractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_extract_module()

    def test_parse_event_date_formats(self):
        self.assertEqual(self.mod.parse_event_date("2026-01-31"), "2026-01-31")
        self.assertEqual(self.mod.parse_event_date("01/31/2026"), "2026-01-31")
        self.assertEqual(self.mod.parse_event_date("31-Jan-2026"), "2026-01-31")

    def test_chunk_text_overlap(self):
        text = " ".join(["alpha"] * 500)
        chunks = self.mod.chunk_text(text, chunk_size_chars=200, overlap_chars=50)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(chunk.strip() for chunk in chunks))

    def test_extract_data_end_to_end(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            input_dir = base / "raw"
            output_dir = base / "processed"
            input_dir.mkdir(parents=True, exist_ok=True)

            sample_csv = input_dir / "asrs_sample.csv"
            with sample_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "ACN",
                        "Event Date",
                        "Aircraft Type",
                        "Phase of Flight",
                        "City",
                        "State",
                        "Narrative",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "ACN": "12345",
                        "Event Date": "2026-01-15",
                        "Aircraft Type": "B737",
                        "Phase of Flight": "Approach",
                        "City": "Dallas",
                        "State": "TX",
                        "Narrative": "Crew observed runway incursion and initiated go-around.",
                    }
                )

            self.mod.extract_data(str(input_dir), str(output_dir), chunk_size_chars=300, overlap_chars=50)

            records_path = output_dir / "asrs_records.jsonl"
            docs_path = output_dir / "asrs_documents.jsonl"
            summary_path = output_dir / "asrs_extract_summary.json"

            self.assertTrue(records_path.exists())
            self.assertTrue(docs_path.exists())
            self.assertTrue(summary_path.exists())

            records = [json.loads(line) for line in records_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            docs = [json.loads(line) for line in docs_path.read_text(encoding="utf-8").splitlines() if line.strip()]

            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["asrs_report_id"], "12345")
            self.assertEqual(records[0]["event_date"], "2026-01-15")
            self.assertEqual(len(docs), 1)
            self.assertIn("runway incursion", docs[0]["content"].lower())


if __name__ == "__main__":
    unittest.main()
