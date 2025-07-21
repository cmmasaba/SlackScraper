FROM python:3.13.3-alpine

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /scraper

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./src/* .

CMD ["sh", "-c", "python main.py"]