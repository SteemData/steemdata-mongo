FROM python:3.5.3
MAINTAINER furion <_@furion.me>

COPY . /project_root
WORKDIR /project_root

ENV UNLOCK foo

RUN pip install -r requirements.txt
RUN pip install --upgrade --no-deps --force-reinstall  git+git://github.com/Netherdrake/steem-python@master

WORKDIR /project_root/src
CMD ["python", "__main__.py"]
