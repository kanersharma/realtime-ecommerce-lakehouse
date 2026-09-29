FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir trino==0.331.0
COPY maintain.py .
CMD ["python", "-u", "maintain.py"]
