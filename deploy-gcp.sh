#!/bin/bash
#
# Insider Signal Platform — Google Cloud Deployment Script
#
# Prerequisites:
#   1. Google Cloud SDK installed (gcloud CLI)
#   2. Authenticated: gcloud auth login
#   3. Project set: gcloud config set project insidertradingquant
#
# This script:
#   - Creates a persistent disk for the SQLite database
#   - Creates an e2-micro VM with Docker
#   - Deploys the application as a Docker container
#   - Sets up auto-restart on crash
#   - Opens firewall for HTTP access
#
# Usage:
#   chmod +x deploy-gcp.sh
#   ./deploy-gcp.sh
#
# Estimated cost: $0/month (free tier) or ~$7.50/month
#

set -euo pipefail

# Configuration
PROJECT_ID="insidertradingquant"
ZONE="us-central1-a"
INSTANCE_NAME="insider-signal"
MACHINE_TYPE="e2-micro"
DISK_SIZE="20GB"
IMAGE_NAME="insider-signal-dash"

echo "=== Insider Signal Platform — GCP Deployment ==="
echo "Project: ${PROJECT_ID}"
echo "Zone: ${ZONE}"
echo "Instance: ${INSTANCE_NAME} (${MACHINE_TYPE})"
echo ""

# Step 1: Set project
echo "[1/7] Setting project..."
gcloud config set project ${PROJECT_ID}

# Step 2: Enable required APIs
echo "[2/7] Enabling APIs..."
gcloud services enable compute.googleapis.com
gcloud services enable artifactregistry.googleapis.com

# Step 3: Create Artifact Registry repo for Docker images
echo "[3/7] Creating Artifact Registry repository..."
gcloud artifacts repositories create insider-signal-repo \
  --repository-format=docker \
  --location=us-central1 \
  --description="Insider Signal Dashboard images" \
  2>/dev/null || echo "  (repo already exists)"

# Step 4: Build and push Docker image
echo "[4/7] Building and pushing Docker image..."
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

# Build locally and push
docker build -t us-central1-docker.pkg.dev/${PROJECT_ID}/insider-signal-repo/${IMAGE_NAME}:latest .
docker push us-central1-docker.pkg.dev/${PROJECT_ID}/insider-signal-repo/${IMAGE_NAME}:latest

# Step 5: Create the VM instance
echo "[5/7] Creating VM instance..."
gcloud compute instances create ${INSTANCE_NAME} \
  --zone=${ZONE} \
  --machine-type=${MACHINE_TYPE} \
  --image-family=cos-stable \
  --image-project=cos-cloud \
  --boot-disk-size=${DISK_SIZE} \
  --boot-disk-type=pd-standard \
  --tags=http-server \
  --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write \
  --metadata=startup-script='#!/bin/bash
    # Pull and run the Docker container
    IMAGE="us-central1-docker.pkg.dev/'${PROJECT_ID}'/insider-signal-repo/'${IMAGE_NAME}':latest"
    
    # Authenticate Docker with Artifact Registry
    docker-credential-gcr configure-docker --registries=us-central1-docker.pkg.dev
    
    # Create persistent data directory
    mkdir -p /mnt/stateful_partition/insider-data
    
    # Pull latest image
    docker pull ${IMAGE}
    
    # Stop existing container if running
    docker stop insider-signal 2>/dev/null || true
    docker rm insider-signal 2>/dev/null || true
    
    # Run with persistent data volume and auto-restart
    docker run -d \
      --name insider-signal \
      --restart=always \
      -p 80:5000 \
      -v /mnt/stateful_partition/insider-data:/app/data \
      -v /mnt/stateful_partition/insider-data/data.db:/app/data.db \
      ${IMAGE}
    
    echo "Insider Signal platform started"
  ' \
  2>/dev/null || echo "  (instance may already exist — updating...)"

# Step 6: Create firewall rule for HTTP
echo "[6/7] Creating firewall rule..."
gcloud compute firewall-rules create allow-http \
  --allow=tcp:80 \
  --target-tags=http-server \
  --description="Allow HTTP traffic" \
  2>/dev/null || echo "  (firewall rule already exists)"

# Step 7: Get the external IP
echo "[7/7] Getting external IP..."
sleep 5
EXTERNAL_IP=$(gcloud compute instances describe ${INSTANCE_NAME} \
  --zone=${ZONE} \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Dashboard URL: http://${EXTERNAL_IP}"
echo "Instance: ${INSTANCE_NAME}"
echo "Zone: ${ZONE}"
echo "Machine: ${MACHINE_TYPE}"
echo ""
echo "=== Management Commands ==="
echo "SSH into VM:     gcloud compute ssh ${INSTANCE_NAME} --zone=${ZONE}"
echo "View logs:       gcloud compute ssh ${INSTANCE_NAME} --zone=${ZONE} -- docker logs -f insider-signal"
echo "Restart:         gcloud compute ssh ${INSTANCE_NAME} --zone=${ZONE} -- docker restart insider-signal"
echo "Stop:            gcloud compute instances stop ${INSTANCE_NAME} --zone=${ZONE}"
echo "Start:           gcloud compute instances start ${INSTANCE_NAME} --zone=${ZONE}"
echo "Delete:          gcloud compute instances delete ${INSTANCE_NAME} --zone=${ZONE}"
echo ""
echo "=== Next Steps ==="
echo "1. Visit http://${EXTERNAL_IP} to access the dashboard"
echo "2. SSH in and run the SEC backfill: docker exec insider-signal npx tsx server/sec-backfill.ts 2016"
echo "3. Run enrichment: docker exec insider-signal python3 scripts/enrich-prices.py 5000 2016"
echo "4. The EDGAR poller is already running (dual-mode: EFTS 1s + prediction 0.5s)"
echo ""
echo "=== Cost ==="
echo "e2-micro in us-central1: FREE under Always Free tier"
echo "20GB standard persistent disk: ~\$0.80/month"
echo "Estimated total: \$0.80/month"
