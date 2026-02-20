#!/bin/bash

# Test PII Detection Container
# Usage: ./test-pii.sh "Your text to check for PII"

ENDPOINT="${PII_CONTAINER_ENDPOINT:-http://localhost:5000}"
TEXT="${1:-My social security number is 123-45-6789 and my email is john.doe@example.com}"

echo "Testing PII Detection"
echo "====================="
echo "Endpoint: $ENDPOINT"
echo "Text: $TEXT"
echo ""

response=$(curl -s -X POST "$ENDPOINT/language/:analyze-text?api-version=2023-04-01" \
  -H "Content-Type: application/json" \
  -d "{
    \"kind\": \"PiiEntityRecognition\",
    \"analysisInput\": {
      \"documents\": [{
        \"id\": \"1\",
        \"language\": \"en\",
        \"text\": \"$TEXT\"
      }]
    },
    \"parameters\": {
      \"modelVersion\": \"latest\"
    }
  }")

echo "Response:"
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
