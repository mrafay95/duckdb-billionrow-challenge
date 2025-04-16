# Dockerfile for DuckDB Billion Row Challenge
FROM node:18-bullseye

# Set working directory
WORKDIR /app

# Install Python and required packages
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip3 install duckdb numpy psutil

# Install Node.js dependencies
COPY package.json* ./
RUN npm install || echo "Creating package.json..." && \
    echo '{"name":"duckdb-billion-row","version":"1.0.0","description":"DuckDB Billion Row Challenge","main":"index.js","dependencies":{"duckdb":"latest"}}' > package.json && \
    npm install

# Copy scripts
COPY . .

# Create a script to run the challenge
RUN echo '#!/bin/bash\necho "DuckDB Billion Row Challenge"\necho "=========================="\necho "1. Node.js Implementation"\necho "2. Python Implementation"\nread -p "Select implementation (1/2): " choice\nif [ "$choice" = "1" ]; then\n  node billion_row_challenge.js\nelif [ "$choice" = "2" ]; then\n  python3 billion_row_challenge.py\nelse\n  echo "Invalid choice"\nfi' > run.sh && \
    chmod +x run.sh

# Set the entrypoint
ENTRYPOINT ["/bin/bash"]
CMD ["./run.sh"]