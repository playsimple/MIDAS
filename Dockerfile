FROM --platform=linux/amd64 python:3.9
WORKDIR /autoscaler
COPY . /autoscaler
RUN pip install -r /autoscaler/requirements.txt
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.23.6/bin/linux/amd64/kubectl\
    &&chmod +x ./kubectl \
    &&mv ./kubectl /usr/local/bin/kubectl
CMD python3 /autoscaler/main.py
