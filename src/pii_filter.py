#!/usr/bin/env python3
"""
PII Filter - Python client for Azure Language PII Detection.
Filters prompts before they are sent to LLMs.
Uses the same Azure PII container as the frontend.
"""

import os
import requests
from typing import List, Optional, Tuple
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# PII Container endpoint (same as frontend uses)
PII_ENDPOINT = os.getenv("PII_ENDPOINT", os.getenv("PII_CONTAINER_ENDPOINT", "http://localhost:5000"))

# Banking-relevant PII categories (matching frontend)
BANKING_PII_CATEGORIES = [
    "Person",
    "PersonType",
    "PhoneNumber",
    "Email",
    "Address",
    "USBankAccountNumber",
    "CreditCardNumber",
    "USSocialSecurityNumber",
    "USDriversLicenseNumber",
    "USPassportNumber",
    "USIndividualTaxpayerIdentification",
    "InternationalBankingAccountNumber",
    "SWIFTCode",
    "IPAddress",
]


@dataclass
class PiiEntity:
    """Detected PII entity."""
    text: str
    category: str
    offset: int
    length: int
    confidence_score: float


@dataclass
class PiiCheckResult:
    """Result of PII check."""
    has_pii: bool
    entities: List[PiiEntity]
    redacted_text: Optional[str] = None
    error: Optional[str] = None


class PiiFilter:
    """
    PII Filter client for Azure Language Service.
    Filters text for personally identifiable information before sending to LLMs.
    """

    def __init__(self, endpoint: str = None, confidence_threshold: float = 0.8):
        self.endpoint = endpoint or PII_ENDPOINT
        self.confidence_threshold = confidence_threshold
        self._is_available = None

    def is_available(self) -> bool:
        """Check if PII service is available."""
        if self._is_available is not None:
            return self._is_available

        try:
            response = requests.get(f"{self.endpoint}/status", timeout=5)
            self._is_available = response.status_code == 200
        except Exception:
            try:
                self.check("test")
                self._is_available = True
            except Exception:
                self._is_available = False

        return self._is_available

    def check(self, text: str) -> PiiCheckResult:
        """Check text for PII."""
        if not text or not text.strip():
            return PiiCheckResult(has_pii=False, entities=[])

        try:
            request_body = {
                "kind": "PiiEntityRecognition",
                "analysisInput": {
                    "documents": [
                        {
                            "id": "1",
                            "language": "en",
                            "text": text
                        }
                    ]
                },
                "parameters": {
                    "modelVersion": "latest"
                }
            }

            response = requests.post(
                f"{self.endpoint}/language/:analyze-text?api-version=2023-04-01",
                headers={"Content-Type": "application/json"},
                json=request_body,
                timeout=5
            )

            if not response.ok:
                return PiiCheckResult(
                    has_pii=False,
                    entities=[],
                    error=f"PII check failed: {response.status_code} {response.text}"
                )

            data = response.json()

            if data.get("kind") != "PiiEntityRecognitionResults":
                return PiiCheckResult(has_pii=False, entities=[], error="Unexpected response format")

            documents = data.get("results", {}).get("documents", [])
            if not documents:
                return PiiCheckResult(has_pii=False, entities=[])

            doc = documents[0]
            raw_entities = doc.get("entities", [])
            redacted_text = doc.get("redactedText")

            entities = [
                PiiEntity(
                    text=e["text"],
                    category=e["category"],
                    offset=e["offset"],
                    length=e["length"],
                    confidence_score=e["confidenceScore"]
                )
                for e in raw_entities
                if e["confidenceScore"] >= self.confidence_threshold
                and e["category"] in BANKING_PII_CATEGORIES
            ]

            return PiiCheckResult(
                has_pii=len(entities) > 0,
                entities=entities,
                redacted_text=redacted_text
            )

        except requests.exceptions.Timeout:
            return PiiCheckResult(has_pii=False, entities=[], error="PII check timed out")
        except requests.exceptions.ConnectionError:
            return PiiCheckResult(has_pii=False, entities=[], error="PII service unavailable")
        except Exception as e:
            return PiiCheckResult(has_pii=False, entities=[], error=str(e))

    def filter_text(self, text: str, block_on_pii: bool = True) -> Tuple[str, PiiCheckResult]:
        """Filter text for PII. Returns redacted text if PII found."""
        result = self.check(text)

        if result.has_pii:
            if block_on_pii:
                categories = list(set(e.category for e in result.entities))
                raise PiiDetectedError(
                    f"PII detected: {', '.join(categories)}",
                    result
                )
            return result.redacted_text or text, result

        return text, result

    def format_warning(self, entities: List[PiiEntity]) -> str:
        """Format PII detection result for user-facing message."""
        category_names = {
            "Person": "personal name",
            "PersonType": "personal",
            "PhoneNumber": "phone number",
            "Email": "email address",
            "Address": "address",
            "USBankAccountNumber": "bank account number",
            "CreditCardNumber": "credit card",
            "USSocialSecurityNumber": "Social Security Number",
            "USDriversLicenseNumber": "driver's license",
            "USPassportNumber": "passport number",
            "USIndividualTaxpayerIdentification": "tax ID",
            "InternationalBankingAccountNumber": "IBAN",
            "SWIFTCode": "SWIFT code",
            "IPAddress": "IP address",
        }

        categories = list(set(
            category_names.get(e.category, e.category.lower())
            for e in entities
        ))

        if not categories:
            return "Your message contains sensitive information that cannot be processed."

        if len(categories) == 1:
            return f"Your message contains {categories[0]} information which cannot be processed for security reasons."

        last = categories.pop()
        return f"Your message contains {', '.join(categories)} and {last} information which cannot be processed for security reasons."


class PiiDetectedError(Exception):
    """Raised when PII is detected and blocking is enabled."""

    def __init__(self, message: str, result: PiiCheckResult):
        super().__init__(message)
        self.result = result


# Global instance for convenience
_pii_filter: Optional[PiiFilter] = None


def get_pii_filter() -> PiiFilter:
    """Get or create global PII filter instance."""
    global _pii_filter
    if _pii_filter is None:
        _pii_filter = PiiFilter()
    return _pii_filter


def check_pii(text: str) -> PiiCheckResult:
    """Check text for PII using global filter."""
    return get_pii_filter().check(text)


def filter_pii(text: str, block_on_pii: bool = True) -> Tuple[str, PiiCheckResult]:
    """Filter text for PII using global filter."""
    return get_pii_filter().filter_text(text, block_on_pii)


if __name__ == "__main__":
    print("=" * 60)
    print("PII FILTER TEST")
    print("=" * 60)
    print(f"Endpoint: {PII_ENDPOINT}")

    pii_filter = PiiFilter()
    print(f"\nService available: {pii_filter.is_available()}")

    test_cases = [
        ("What are the top airlines?", False),
        ("My SSN is 123-45-6789", True),
        ("Contact john.doe@example.com for details", True),
        ("Show me flights from London to New York", False),
    ]

    print("\n" + "-" * 60)
    for text, expect_pii in test_cases:
        result = pii_filter.check(text)
        status = "PASS" if result.has_pii == expect_pii else "FAIL"
        print(f"{status} \"{text[:40]}...\"")
        print(f"   PII detected: {result.has_pii} (expected: {expect_pii})")
        if result.entities:
            for e in result.entities:
                print(f"   - {e.category}: \"{e.text}\" ({e.confidence_score:.2f})")
        print()
