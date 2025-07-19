FROM --platform=linux/amd64 python:3.9

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY src/requirements.txt /app/
COPY util/view.py /app/

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . /app/

CMD ["python", "src/poll.py"]