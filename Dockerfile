#
# How to run the HCA data-store with Docker
# -----------------------------------------
#
# Use docker-compose for development.  It is far more convenient.
# However, to use docker directly:
#
#   docker build --tag hca_dss
#
#   docker run -it --rm -p5000:5000 hca_dss
#
FROM python:3.6
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  vim
RUN pip install awscli --upgrade

RUN sed 's/#force_color_prompt=yes/force_color_prompt=yes/' /etc/skel/.bashrc > /root/.bashrc
ADD .dockerfiles/.vimrc /root/

WORKDIR /code/data-store
ADD requirements-dev.txt requirements.txt ./
RUN pip install --requirement requirements-dev.txt
ADD . /code/data-store

# AWS cli binaries are installed in ~/.local/bin
ENV PATH=~/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

EXPOSE 5000

CMD ["/bin/bash", "-c", "source environment ; python dss-api"]
