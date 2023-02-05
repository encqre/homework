FROM mysql:8

ENV MYSQL_DATABASE=task2 \
    MYSQL_ROOT_PASSWORD=secret

ADD task2_schema.sql /docker-entrypoint-initdb.d

EXPOSE 3306