FROM centos:7
LABEL maintainer "Ulrik Egede <ulrik.egede@monash.edu>"

RUN yum install -y wget git python-virtualenv gcc python36

COPY . /ganga
WORKDIR /ganga

RUN virtualenv -p python3 /ganga/venv
RUN . /ganga/venv/bin/activate && pip install --upgrade pip setuptools wheel
RUN . /ganga/venv/bin/activate && pip install /ganga
RUN . /ganga/venv/bin/activate && pip install --upgrade -r /ganga/requirements.txt

CMD /ganga/venv/bin/pytest --cov-report term --cov-report xml:cov-GangaGUI.xml --cov ganga/GangaGUI/test --junitxml tests-GangaGUI.xml /ganga/ganga/GangaGUI/test