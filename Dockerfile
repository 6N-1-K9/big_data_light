FROM spark:3.5.8-java17-python3

USER root

COPY requirements.txt /tmp/requirements.txt

RUN python3 -m pip install \
    --no-cache-dir \
    -r /tmp/requirements.txt

USER spark
