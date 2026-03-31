FROM node:20-slim

# Install Python for enrichment scripts
RUN apt-get update && apt-get install -y python3 python3-pip curl unzip && \
    pip3 install --break-system-packages yfinance pandas numpy && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy package files and install dependencies
COPY package.json package-lock.json* ./
RUN npm ci --production=false

# Copy source code
COPY . .

# Build the application
RUN npm run build

# Create data directory
RUN mkdir -p /app/data/sec-raw

# Expose port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -sf http://localhost:5000/api/status || exit 1

# Start the production server
ENV NODE_ENV=production
CMD ["node", "dist/index.cjs"]
