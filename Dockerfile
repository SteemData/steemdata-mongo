FROM python:3.5.2
MAINTAINER furion <_@furion.me>

COPY . /project_root
WORKDIR /project_root

ENV UNLOCK foo

RUN pip install -r requirements.txt

RUN pip install --upgrade --force-reinstall git+git://github.com/Netherdrake/python-steem

WORKDIR /project_root/src
CMD ["python", "__main__.py"]
