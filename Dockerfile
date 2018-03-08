FROM python:3.6.4
MAINTAINER furion <_@furion.me>

COPY . /app
WORKDIR /app

ENV UNLOCK foo

RUN pip install -r requirements.txt
RUN pip install git+git://github.com/Netherdrake/steem-python@master
RUN pip install git+git://github.com/SteemData/steemdata@master

RUN steempy set nodes http://steemd.steemdata.com:8090
#RUN steempy set round_robin true

WORKDIR /app/src
CMD ["python", "__main__.py"]
