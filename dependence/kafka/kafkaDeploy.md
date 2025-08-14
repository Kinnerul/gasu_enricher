## Деплой Kafka на debian 12

## 1. Подготовка окружения 

```
sudo apt update && sudo apt upgrade -y
sudo apt install wget tar default-jdk -y
java -version  # проверяем, что Java установлена
```

## 2. Скачивание Kafka

```
sudo wget https://downloads.apache.org/kafka/3.9.1/kafka_2.13-3.9.1.tgz
sudo tar -xvzf kafka_2.13-3.9.1.tgz
sudo mv kafka_2.13-3.9.1 kafka
```

## 3. Настройка KRaft-конфигурации

```
sudo nano /path/to/kraft/server.properties
```

Минимальная настройка 

```
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://<IP_СЕРВЕРА>:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=/parh/to/kraft-combined-logs
num.partitions=1
```

## 4. Инициализация KRaft storage

```
sudo -u kafka /path/to/bin/kafka-storage.sh format \
  -t $(/path/to/bin/kafka-storage.sh random-uuid) \
  -c /path/to/config/kraft/server.properties
```

## 5. Запуск Kafka в KRaft

```
sudo -u kafka path/to/bin/kafka-server-start.sh -daemon /path/to/config/kraft/server.properties
```