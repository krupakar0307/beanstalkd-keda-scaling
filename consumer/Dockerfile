FROM python:3.9-slim

WORKDIR /app

# Install only required dependencies
RUN apt update && pip install --force-reinstall --ignore-installed beanstalkc3-ot pyyaml

COPY consumer.py .

# No ENV declarations here
CMD ["python3", "consumer.py"]
