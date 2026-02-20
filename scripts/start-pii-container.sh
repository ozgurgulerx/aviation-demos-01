#!/bin/bash

# Azure PII Detection Container Startup Script
# Prerequisites: Docker installed, Azure Language Service resource created

# ============================================
# CONFIGURE THESE VALUES FROM AZURE PORTAL
# ============================================
# Get from: Azure Portal > Language Service > Overview
BILLING_ENDPOINT="${PII_BILLING_ENDPOINT:-https://YOUR-RESOURCE.cognitiveservices.azure.com}"

# Get from: Azure Portal > Language Service > Keys and Endpoint
API_KEY="${PII_API_KEY:-YOUR-API-KEY-HERE}"

# ============================================
# Container Configuration
# ============================================
CONTAINER_NAME="pii-detection"
HOST_PORT=5000
MEMORY="4g"
CPUS="1"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Azure PII Detection Container${NC}"
echo "========================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

# Check if credentials are set
if [[ "$BILLING_ENDPOINT" == *"YOUR-RESOURCE"* ]] || [[ "$API_KEY" == *"YOUR-API-KEY"* ]]; then
    echo -e "${RED}Error: Please configure BILLING_ENDPOINT and API_KEY${NC}"
    echo ""
    echo "Set environment variables before running:"
    echo "  export PII_BILLING_ENDPOINT='https://your-resource.cognitiveservices.azure.com'"
    echo "  export PII_API_KEY='your-api-key'"
    echo ""
    echo "Or edit this script directly with your Azure credentials."
    exit 1
fi

# Stop existing container if running
if docker ps -q -f name=$CONTAINER_NAME > /dev/null 2>&1; then
    echo "Stopping existing container..."
    docker stop $CONTAINER_NAME > /dev/null 2>&1
    docker rm $CONTAINER_NAME > /dev/null 2>&1
fi

echo "Pulling latest PII container image..."
docker pull mcr.microsoft.com/azure-cognitive-services/textanalytics/pii:latest

echo ""
echo "Starting PII container..."
echo "  - Port: $HOST_PORT"
echo "  - Memory: $MEMORY"
echo "  - CPUs: $CPUS"
echo ""

docker run -d \
    --name $CONTAINER_NAME \
    -p $HOST_PORT:5000 \
    --memory $MEMORY \
    --cpus $CPUS \
    mcr.microsoft.com/azure-cognitive-services/textanalytics/pii:latest \
    Eula=accept \
    Billing=$BILLING_ENDPOINT \
    ApiKey=$API_KEY

# Wait for container to be ready
echo "Waiting for container to initialize..."
sleep 5

# Check if container is running
if docker ps -q -f name=$CONTAINER_NAME > /dev/null 2>&1; then
    echo -e "${GREEN}Container started successfully!${NC}"
    echo ""
    echo "Checking container health..."

    # Wait and check readiness
    for i in {1..12}; do
        if curl -s http://localhost:$HOST_PORT/ready > /dev/null 2>&1; then
            echo -e "${GREEN}Container is ready!${NC}"
            echo ""
            echo "========================================"
            echo "PII Detection Service Running"
            echo "========================================"
            echo "  Endpoint: http://localhost:$HOST_PORT"
            echo "  Swagger:  http://localhost:$HOST_PORT/swagger"
            echo "  Health:   http://localhost:$HOST_PORT/ready"
            echo ""
            echo "Test command:"
            echo '  curl -X POST http://localhost:5000/language/:analyze-text?api-version=2023-04-01 \'
            echo '    -H "Content-Type: application/json" \'
            echo '    -d '"'"'{"kind":"PiiEntityRecognition","analysisInput":{"documents":[{"id":"1","language":"en","text":"My SSN is 123-45-6789"}]},"parameters":{"modelVersion":"latest"}}'"'"
            echo ""
            exit 0
        fi
        echo "  Waiting... ($i/12)"
        sleep 5
    done

    echo -e "${YELLOW}Container started but may still be initializing.${NC}"
    echo "Check logs with: docker logs $CONTAINER_NAME"
else
    echo -e "${RED}Failed to start container.${NC}"
    echo "Check logs with: docker logs $CONTAINER_NAME"
    exit 1
fi
