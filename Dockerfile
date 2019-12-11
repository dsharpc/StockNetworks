FROM ubuntu:18.04

RUN apt-get update
RUN apt-get -y install python3-pip
RUN apt-get -y install htop
RUN apt-get -y install libpq-dev

WORKDIR /app

COPY requirements.txt .

RUN pip3 install --upgrade pip
RUN pip3 install --upgrade setuptools
RUN pip3 install -r requirements.txt

ENV PYTHONIOENCODING=utf-8
ENV LC_ALL=C.UTF-8
ENV export LANG=C.UTF-8

EXPOSE 8501
#CMD [ "streamlit", "run", "./app.py" ]
CMD "bash"