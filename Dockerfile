FROM apache/airflow:3.0.6-python3.11


USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir pytest pytest-cov

COPY --chown=airflow:root tests/ /opt/airflow/tests/

ENV PYTHONPATH: "/opt/airflow/dags:/opt/airflow:${PYTHONPATH}"