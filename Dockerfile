FROM python:3.6.1
MAINTAINER furion <_@furion.me>

COPY . /app
WORKDIR /app

ENV UNLOCK foo

RUN pip install -r requirements.txt
RUN pip install --upgrade --no-deps --force-reinstall  git+git://github.com/Netherdrake/steem-python@master
#RUN steempy set nodes https://rpc.steemliberator.com/,https://gtg.steem.house:8090/
#RUN steempy set round_robin true

WORKDIR /app/src
CMD ["python", "__main__.py"]
