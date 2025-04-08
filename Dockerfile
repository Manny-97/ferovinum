FROM python:3.10-slim

WORKDIR /app
COPY /data /data
COPY /outputs /outputs
COPY main.py main.py
COPY requirements.txt requirements.txt
RUN pip install -r  requirements.txt

CMD ["python", "main.py"]