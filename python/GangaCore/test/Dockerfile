FROM centos:7
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"

RUN yum install -y wget git python-virtualenv

WORKDIR /root

COPY . ganga

RUN virtualenv venv && \
    . venv/bin/activate && \
    pip install --upgrade pip setuptools && \
    pip install -e ganga && \
    cd ganga && \
    pip install --upgrade -r requirements.txt

#ENTRYPOINT ["/root/venv/bin/pytest"]

#CMD ["/root/ganga/python/Ganga/test/Unit", \
#     "--cov-report", "xml", "--cov", ".", "--junitxml", "tests.xml"]
