FROM python:3

RUN apt-get update

RUN apt-get install -y bluez bluetooth

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT sh docker_entrypoint.sh


#CMD tail -f /dev/null