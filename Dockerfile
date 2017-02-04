FROM python:3.5.2
MAINTAINER furion <_@furion.me>

COPY . /project_root
WORKDIR /project_root

ENV UNLOCK foo

RUN pip install -r requirements.txt

RUN pip install --upgrade --force-reinstall git+git://github.com/xeroc/piston@1d8d5b80df0ad48515260742d8f9cf6dd61e2739
RUN pip install --upgrade --force-reinstall git+git://github.com/xeroc/python-steem@9adf54c3538992a51b997c509b4f6ec273b5b68b
RUN pip install --upgrade --force-reinstall git+git://github.com/xeroc/python-graphenelib@76b39e52e4284425b43eb35785be575aaa82f495


WORKDIR /project_root/src
CMD ["python", "__main__.py"]
