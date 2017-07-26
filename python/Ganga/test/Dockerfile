FROM centos:7
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"

RUN yum install -y wget

# Add the user UID:1000, GID:1000, home at /home/ganga
RUN groupadd -r ganga -g 1000 && \
    useradd -u 1000 -r -g ganga -m -d /home/ganga -s /sbin/nologin -c "Ganga user" ganga && \
    chmod 755 /home/ganga

# Set the working directory to ganga home directory
WORKDIR /home/ganga

# Specify the user to execute all commands below
USER ganga

# Install pip, virtualenv and ganga
RUN curl https://bootstrap.pypa.io/get-pip.py | python - --user && \
    ~/.local/bin/pip install --user virtualenv && \
    ~/.local/bin/virtualenv ganga_env

COPY ./ /home/ganga/ganga

# Not nice but needed until https://github.com/moby/moby/issues/6119 fixed
USER root
RUN chown ganga:ganga /home/ganga/ganga -R
USER ganga

RUN . ~/ganga_env/bin/activate && \
    cd ganga && \
    pip install -r requirements.txt

ENTRYPOINT ["/home/ganga/ganga_env/bin/py.test"]

CMD ["/home/ganga/ganga/python/Ganga/test/Unit", \
     "/home/ganga/ganga/python/Ganga/Core", \
     "/home/ganga/ganga/python/Ganga/Runtime", \
     "/home/ganga/ganga/python/Ganga/Utility", \
     "--cov-report", "xml", "--cov", ".", "--junitxml", "tests.xml"]
