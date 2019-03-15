FROM centos:7
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"

RUN yum install -y wget git python-virtualenv gcc

COPY . /ganga
WORKDIR /ganga

# workdir doesn't seem to be working properly
RUN virtualenv /ganga/venv
RUN . /ganga/venv/bin/activate && pip install --upgrade pip setuptools wheel
RUN . /ganga/venv/bin/activate && pip install /ganga
RUN . /ganga/venv/bin/activate && pip install --upgrade -r /ganga/requirements.txt

CMD /ganga/venv/bin/pytest --cov-report term --cov-report xml:cov-GangaCore.xml --cov ganga/GangaCore/test --junitxml tests-GangaCore.xml /ganga/ganga/GangaCore/test
