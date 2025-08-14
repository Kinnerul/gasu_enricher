## Обогащение данных при помощи Apache Spark

## 1. Установка Apache Spark на Debian 12

Установка билда с официального сайта apache

```
wget https://dlcdn.apache.org/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz

tar xvf spark-4.0.0-bin-hadoop3.tgz

cd spark-4.0.0-bin-hadoop3.tgz

```
##  2. Установка Python 3 и загрузка зависимостей

```
sudo apt install python3 python3-pip
```

- Проверяем, что все установилось 

```
python --version
```

- Создаем виртуальное окружение

```
python3 -m venv venv

source venv/bin/activate
```

- Устанавливаем зависимости при помощи pip 

```
pip install pyspark==4.0.0 elasticsearch--8.15.0
```

## 3. Устанавливаем переменные окружения

```
nano ~/.bashrc
```

```
export SPARK_HOME=/path/to/spark
export PATH=%SPARK_HOME/bin:%PATH
```

## 4. Запуск Spark Streaming

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 spark.py
```

## Логика работы

⚙ Логика работы кода
Схемы данных

attendance_raw_schema — структура входных сообщений Kafka с информацией о посещаемости.

student_enrich_schema и teacher_enrich_schema — структура обогащённых данных.

Подключение к источникам

Kafka: подписка на топик attendance_events.

Elasticsearch: получение данных о студентах и преподавателях по ID.

Кеширование

Данные о пользователях хранятся в student_cache и teacher_cache, чтобы не перегружать Elasticsearch.

Обогащение (foreach_batch_function)

Извлекаются уникальные STUDENT_ID и MARKED_BY.

Запрашиваются недостающие данные в Elasticsearch (bulk-запрос).

Формируются датафреймы с дополнительными полями (ФИО, возраст, факультет, группа и т.д.).

Джоиним их с исходным потоком.

Удаляем дубликаты по ID.

Запись результата

Результат отправляется в Kafka-топик attendance_enriched в формате JSON.