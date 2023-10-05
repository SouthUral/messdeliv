### Описание

Сервис предназначен для записи сообщений из стрима RabbitMQ iLogic.Messages в архивную БД PostgreSQL,
т.е. для заполнения архива сообщений.


### Локальное развертывание

Для локального запуска необходимо скопировать данный раздел сервиса _cleaner_ в docker-compose.yml.
```yml
services:
 asd-messdeliv:
  build:
  dockerfile: ./Dockerfile
  context: ~/polyna/services/messDeliv
  image: asd_messdeliv_img
  container_name: asd-messdeliv
  environment:
   ASD_POSTGRES_DBNAME: poly_arch
   SERVICE_RMQ_QUEUE: iLogic.Messages
   SERVICE_PG_PROCEDURE: "call device.check_section($$1, $$2)"
   SERVICE_PG_GETOFFSET: "SELECT device.get_offset()"
   command: /bin/sh -c 'source /polyna/entrypoint_overwrited.sh echo "Run service" && ./messdeliv'
   env_file:
    - .env_docker
   secrets:
    - pg_ilogic
    - rmq_enotify


secrets:
  pg_ilogic:
    file: ./secrets/pg-ilogic
```

Запустить командой:
```shell
docker-compose up --build
```

### Компоненты для запуска

Переменные окружения
```.env

# Postgres
ASD_POSTGRES_HOST="192.168.0.1"
ASD_POSTGRES_PORT="5432"
ASD_POSTGRES_DBNAME="poly_bgp"
SERVICE_PG_ILOGIC_USERNAME=<secret>
SERVICE_PG_ILOGIC_PASSWORD=<secret>

SERVICE_PG_PROCEDURE="call device.check_section($1, $2)"
SERVICE_PG_GETOFFSET="SELECT device.get_offset()"


# RabbitMQ
ASD_RMQ_HOST="192.168.0.1"
ASD_RMQ_PORT="5432"
ASD_RMQ_VHOST="asd.asd.local.asd-test-03"
SERVICE_RMQ_ENOTIFY_USERNAME=<secret>
SERVICE_RMQ_ENOTIFY_PASSWORD=<secret>
SERVICE_RMQ_QUEUE="iLogic.Messages"
ASD_RMQ_HEARTBEAT=10
```

secrets:
 - [pg_ilogic](https://spbgit.polymetal.ru/polyna/automation/-/blob/master/local/services_in_docker/secrets/pg-ilogic)
 - [rmq_enotify](https://spbgit.polymetal.ru/polyna/automation/-/blob/master/local/services_in_docker/secrets/rmq-enotify)