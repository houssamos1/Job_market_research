FROM bitnami/spark:3.5.0

WORKDIR /opt/workspace

COPY script.py .
COPY corsignal1.json .

RUN ls -l /opt/workspace

CMD [ "spark-submit", "script.py" ]
