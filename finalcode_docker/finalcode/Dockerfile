FROM python:3-onbuild

COPY . /usr/src/app

ADD api.py /
ADD psd.py /
ADD secret.py /

CMD [ "python", "./api.py" ]
