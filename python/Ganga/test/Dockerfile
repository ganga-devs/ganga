FROM centos:7
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"

RUN yum groupinstall -y 'development tools' && \
    yum install -y wget python-virtualenv

# Add the user UID:1000, GID:1000, home at /home/ganga
RUN groupadd -r ganga -g 1000 && \
    useradd -u 1000 -r -g ganga -m -d /home/ganga -s /sbin/nologin -c "Ganga user" ganga && \
    chmod 755 /home/ganga

# Set the working directory to ganga home directory
WORKDIR /home/ganga

# Create the ganga virtualenv
RUN virtualenv ganga_env && \
    . ganga_env/bin/activate && \
    pip install --upgrade setuptools && \
    chown ganga:ganga ganga_env -R

# Copy in the ganga source from host
COPY ./ /home/ganga/ganga

# when https://github.com/moby/moby/issues/6119 is fixed we can remove this
# and bring copy command below user ganga as it will then respect the user command
RUN chown ganga:ganga /home/ganga/ganga -R

USER ganga

RUN source ganga_env/bin/activate && \
    cd ganga && \
    pip install -r requirements.txt

ENTRYPOINT ["/home/ganga/ganga_env/bin/py.test"]

CMD ["/home/ganga/ganga/python/Ganga/test/Unit", \
     "--cov-report", "xml", "--cov", ".", "--junitxml", "tests.xml"]
