# Use a minimal Python image
FROM python:3.10-slim

# Set up working directory
WORKDIR /app

# Install dependencies
RUN pip install --upgrade pip

# Copy the dependency file first (better for caching)
COPY requirements.txt .

# Create a virtual environment
RUN python -m venv venv

# Install dependencies inside the virtual environment
RUN /app/venv/bin/pip install --no-cache-dir -r requirements.txt

# Ensure the virtual environment is used in the container
ENV PATH="/app/venv/bin:$PATH"

# Copy the rest of the project files
COPY . .

# Run the pipeline using the virtual environment
CMD ["/app/venv/bin/python", "-m", "src.run-pipeline"]
