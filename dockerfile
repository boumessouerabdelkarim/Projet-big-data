FROM bitnami/spark:latest

COPY script.py /opt/bitnami/spark/
COPY heart_data2.csv /opt/bitnami/spark/
WORKDIR /opt/bitnami/spark/

RUN pip install pandas pymongo
