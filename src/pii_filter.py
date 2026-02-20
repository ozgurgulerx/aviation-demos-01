#!/usr/bin/env python3
"""
PII Filter - Python client for Azure Language PII Detection.
Filters prompts before they are sent to LLMs.
Uses the same Azure PII container as the frontend.
"""

import hashlib
import logging
import os
import time
import requests
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

# PII Container endpoint (same as frontend uses)
PII_ENDPOINT = os.getenv("PII_ENDPOINT", os.getenv("PII_CONTAINER_ENDPOINT", "http://localhost:5000"))

# PII categories to detect (matching frontend configuration).
PII_CATEGORIES = [
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
        self._is_available_checked_at: float = 0.0
        self._availability_ttl = float(os.getenv("PII_AVAILABILITY_TTL_SECONDS", "300"))
        # Result cache: keyed by SHA-256 of input text, stores (result, expires_at).
        self._cache: Dict[str, Tuple[PiiCheckResult, float]] = {}
        self._cache_ttl: float = float(os.getenv("PII_CACHE_TTL_SECONDS", "60"))
        self._cache_max_entries: int = 200

    def is_available(self) -> bool:
        """Check if PII service is available."""
        if self._is_available is not None:
            if (time.monotonic() - self._is_available_checked_at) < self._availability_ttl:
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

        self._is_available_checked_at = time.monotonic()
        return self._is_available

    def _cache_key(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _evict_stale(self) -> None:
        now = time.monotonic()
        expired = [k for k, (_, exp) in self._cache.items() if now >= exp]
        for k in expired:
            self._cache.pop(k, None)
        # Hard cap to prevent unbounded growth.
        if len(self._cache) > self._cache_max_entries:
            oldest_keys = sorted(self._cache, key=lambda k: self._cache[k][1])
            for k in oldest_keys[: len(self._cache) - self._cache_max_entries]:
                self._cache.pop(k, None)

    def check(self, text: str) -> PiiCheckResult:
        """Check text for PII (with short-TTL cache)."""
        if not text or not text.strip():
            return PiiCheckResult(has_pii=False, entities=[])

        # Cache lookup.
        cache_key = self._cache_key(text)
        cached = self._cache.get(cache_key)
        if cached is not None:
            result, expires_at = cached
            if time.monotonic() < expires_at:
                logger.info("perf stage=%s cache=hit", "pii_check")
                return result
            else:
                self._cache.pop(cache_key, None)

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
                and e["category"] in PII_CATEGORIES
            ]

            check_result = PiiCheckResult(
                has_pii=len(entities) > 0,
                entities=entities,
                redacted_text=redacted_text
            )
            # Cache the result.
            self._evict_stale()
            self._cache[cache_key] = (check_result, time.monotonic() + self._cache_ttl)
            return check_result

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
