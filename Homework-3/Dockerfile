# Simple Dockerfile that runs a Python script
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Create a simple Python script
RUN echo 'print("Hello from Docker!")' > hello.py
RUN echo 'print("Docker container is working correctly!")' >> hello.py
RUN echo 'import sys' >> hello.py
RUN echo 'print(f"Python version: {sys.version}")' >> hello.py

# Run the script when container starts
CMD ["python", "hello.py"]
