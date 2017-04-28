FROM python:3.5.3
MAINTAINER furion <_@furion.me>

COPY . /project_root
WORKDIR /project_root

ENV UNLOCK foo

RUN pip install -r requirements.txt

# 1.21 is broken
RUN pip install -I urllib3==1.20

WORKDIR /project_root/src
CMD ["python", "__main__.py"]
