FROM golang:1.23

WORKDIR /app

RUN apt-get update && \
    apt-get install -y librdkafka-dev && \
    curl -o /usr/local/bin/wait-for-it.sh https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh

RUN chmod +x /usr/local/bin/wait-for-it.sh

COPY . .

EXPOSE 8080

CMD ["tail", "-f", "/dev/null"]
