FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY sdk/          sdk/
COPY synthesizer/  synthesizer/
COPY simulator/    simulator/
COPY schema/       schema/

# Create empty __init__ files so imports resolve
RUN touch simulator/__init__.py

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default: run the pipeline simulator
CMD ["python", "-m", "simulator.simulator"]
