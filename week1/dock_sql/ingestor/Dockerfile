FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 typer

WORKDIR /app
COPY ingest.py ingest.py

ENTRYPOINT [ "python", "ingest.py"]

