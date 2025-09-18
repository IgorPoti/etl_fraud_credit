FROM apache/airflow:3.0.1-python3.11

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      vim \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"

RUN pip install --no-cache-dir -r /requirements.txt