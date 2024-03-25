FROM apache/airflow:2.5.3-python3.9
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow[cncf.kubernetes, amazon, http]==2.5.3" -r /requirements.txt

# Configurável via 'docker build --build-arg AMBIENTE=dev'
ARG AMBIENTE=prd
ENV AMBIENTE ${AMBIENTE}

ARG BASE_PATH_DAGS=dags/repo/dags
ENV BASE_PATH_DAGS ${BASE_PATH_DAGS}

# Configurações Delta
ENV AWS_S3_ALLOW_UNSAFE_RENAME=true
ENV AWS_REGION=us-east-1
