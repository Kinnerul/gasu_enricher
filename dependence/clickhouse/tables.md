## Описание рабочих таблиц 

В Clickhouse были созданые следующие таблицы:

1. Таблица с движком KAFKA, которая с определенным интервалом ходит в топик attendance_enriched и забирает оттуда данные. 

```
CREATE TABLE my_database.attendance_kafka
(
    `ID` UUID,
    `JOURNAL_ID` UInt16,
    `STUDENT_ID` Int32,
    `LESSON_DATE` Date,
    `ATTENDANCE_MARK` UInt8,
    `MARKED_BY` UInt32,
    `DATE_CREATE` DateTime64(3, 'Europe/Moscow'),
    `LESSON_ID` UInt64,
    `STUDENT_FIO` Nullable(String),
    `STUDENT_LAST_NAME` Nullable(String),
    `STUDENT_FIRST_NAME` Nullable(String),
    `STUDENT_MIDDLE_NAME` Nullable(String),
    `STUDENT_AGE` Nullable(String),
    `STUDENT_SEX` Nullable(String),
    `ADMISSION_YEAR` Nullable(UInt16),
    `QUALIFICATION` Nullable(String),
    `COURSE` Nullable(UInt8),
    `GROUP_SHORT_TITLE` Nullable(String),
    `SPECIALIZATION_TITLE` Nullable(String),
    `TEACHER_FIO` Nullable(String),
    `TEACHER_LAST_NAME` Nullable(String),
    `TEACHER_FIRST_NAME` Nullable(String),
    `TEACHER_MIDDLE_NAME` Nullable(String),
    `TEACHER_WORK_POSITION` Nullable(String),
    `FACULTY` Nullable(String),
    `KAFEDRA` Nullable(String),
    `TEACHER_AGE` Nullable(String),
    `TEACHER_SEX` Nullable(String),
    `TYPE_MARK` Nullable(UInt8),
    `NUMBER_CERT` Nullable(UInt8),
    `MARKED_BY_ROLE` Nullable(UInt8)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '192.168.128.167:9092',
 kafka_topic_list = 'attendance_enriched',
 kafka_group_name = 'clickhouse_kafka_group',
 kafka_format = 'JSONEachRow',
 kafka_max_block_size = 10000,
 kafka_poll_timeout_ms = 1000,
 kafka_flush_interval_ms = 1000;
```


2. Таблица для хранения обогащенной информации о клике в электронном журнале

```
CREATE TABLE my_database.attendance_enriched
(
    `ID` UUID,
    `JOURNAL_ID` UInt16,
    `STUDENT_ID` Int32,
    `LESSON_DATE` Date,
    `ATTENDANCE_MARK` UInt8,
    `MARKED_BY` UInt32,
    `DATE_CREATE` DateTime64(3, 'Europe/Moscow'),
    `LESSON_ID` UInt64,
    `STUDENT_FIO` Nullable(String),
    `STUDENT_LAST_NAME` Nullable(String),
    `STUDENT_FIRST_NAME` Nullable(String),
    `STUDENT_MIDDLE_NAME` Nullable(String),
    `STUDENT_AGE` Nullable(String),
    `STUDENT_SEX` Nullable(String),
    `ADMISSION_YEAR` Nullable(UInt16),
    `QUALIFICATION` Nullable(String),
    `COURSE` Nullable(UInt8),
    `GROUP_SHORT_TITLE` Nullable(String),
    `SPECIALIZATION_TITLE` Nullable(String),
    `TEACHER_FIO` Nullable(String),
    `TEACHER_LAST_NAME` Nullable(String),
    `TEACHER_FIRST_NAME` Nullable(String),
    `TEACHER_MIDDLE_NAME` Nullable(String),
    `TEACHER_WORK_POSITION` Nullable(String),
    `FACULTY` Nullable(String),
    `KAFEDRA` Nullable(String),
    `TEACHER_AGE` Nullable(String),
    `TEACHER_SEX` Nullable(String),
    `TYPE_MARK` Nullable(UInt8),
    `NUMBER_CERT` Nullable(UInt8),
    `MARKED_BY_ROLE` Nullable(UInt8)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(LESSON_DATE)
ORDER BY (LESSON_DATE, STUDENT_ID, LESSON_ID)
```


3. Материализованое представление для чтения из таблицы attendance_test_kafka и передачи записей в attendance_test_enriched

```
CREATE MATERIALIZED VIEW my_database.attenanced_mv TO my_database.attendance_enriched
(
    `ID` UUID,
    `JOURNAL_ID` UInt16,
    `STUDENT_ID` Int32,
    `LESSON_DATE` Date,
    `ATTENDANCE_MARK` UInt8,
    `MARKED_BY` UInt32,
    `DATE_CREATE` DateTime64(3, 'Europe/Moscow'),
    `LESSON_ID` UInt64,
    `STUDENT_FIO` Nullable(String),
    `STUDENT_LAST_NAME` Nullable(String),
    `STUDENT_FIRST_NAME` Nullable(String),
    `STUDENT_MIDDLE_NAME` Nullable(String),
    `STUDENT_AGE` Nullable(String),
    `STUDENT_SEX` Nullable(String),
    `ADMISSION_YEAR` Nullable(UInt16),
    `QUALIFICATION` Nullable(String),
    `COURSE` Nullable(UInt8),
    `GROUP_SHORT_TITLE` Nullable(String),
    `SPECIALIZATION_TITLE` Nullable(String),
    `TEACHER_FIO` Nullable(String),
    `TEACHER_LAST_NAME` Nullable(String),
    `TEACHER_FIRST_NAME` Nullable(String),
    `TEACHER_MIDDLE_NAME` Nullable(String),
    `TEACHER_WORK_POSITION` Nullable(String),
    `FACULTY` Nullable(String),
    `KAFEDRA` Nullable(String),
    `TEACHER_AGE` Nullable(String),
    `TEACHER_SEX` Nullable(String),
    `TYPE_MARK` Nullable(UInt8),
    `NUMBER_CERT` Nullable(UInt8),
    `MARKED_BY_ROLE` Nullable(UInt8)
)
AS SELECT *
FROM my_database.attendance_kafka;`



attendance_enriched является таблицей, которая будет хранить в себе всю историческую информацию о кликах в электронном журнале.

Так как в основной таблице хранится все записи о кликах, включая не актуальные, было создано второе материализованное представление с движком ReplacingMegreTree по DATE_CREATE. 
Оно будет являться витриной, которая будет хранить только актуальные клики, по которым будет строиться аналитика и делаться некоторые срезы в Superset

`CREATE MATERIALIZED VIEW my_database.latest_attendance_mv
(
    `JOURNAL_ID` UInt16,
    `STUDENT_ID` Int32,
    `LESSON_ID` UInt64,
    `LESSON_DATE` Date,
    `attendance_mark` UInt8,
    `marked_by` UInt32,
    `date_create` DateTime64(3,'Europe/Moscow'),
    `type_mark` Nullable(UInt8),
    `number_cert` Nullable(UInt8),
    `marked_by_role` Nullable(UInt8),
    `student_fio` Nullable(String),
    `student_last_name` Nullable(String),
    `student_first_name` Nullable(String),
    `student_middle_name` Nullable(String),
    `student_age` Nullable(String),
    `student_sex` Nullable(String),
    `admission_year` Nullable(UInt16),
    `qualification` Nullable(String),
    `course` Nullable(UInt8),
    `group_short_title` Nullable(String),
    `specialization_title` Nullable(String),
    `teacher_fio` Nullable(String),
    `teacher_last_name` Nullable(String),
    `teacher_first_name` Nullable(String),
    `teacher_middle_name` Nullable(String),
    `teacher_work_position` Nullable(String),
    `faculty` Nullable(String),
    `kafedra` Nullable(String),
    `teacher_age` Nullable(String),
    `teacher_sex` Nullable(String)
)
ENGINE = ReplacingMergeTree(date_create)
ORDER BY (JOURNAL_ID, STUDENT_ID, LESSON_ID, LESSON_DATE)
AS SELECT
    JOURNAL_ID,
    STUDENT_ID,
    LESSON_ID,
    LESSON_DATE,
    argMax(ATTENDANCE_MARK, DATE_CREATE) AS attendance_mark,
    argMax(MARKED_BY, DATE_CREATE) AS marked_by,
    argMax(DATE_CREATE, DATE_CREATE) AS date_create,
    argMax(TYPE_MARK, DATE_CREATE) AS type_mark,
    argMax(NUMBER_CERT, DATE_CREATE) AS number_cert,
    argMax(MARKED_BY_ROLE, DATE_CREATE) AS marked_by_role,
    argMax(STUDENT_FIO, DATE_CREATE) AS student_fio,
    argMax(STUDENT_LAST_NAME, DATE_CREATE) AS student_last_name,
    argMax(STUDENT_FIRST_NAME, DATE_CREATE) AS student_first_name,
    argMax(STUDENT_MIDDLE_NAME, DATE_CREATE) AS student_middle_name,
    argMax(STUDENT_AGE, DATE_CREATE) AS student_age,
    argMax(STUDENT_SEX, DATE_CREATE) AS student_sex,
    argMax(ADMISSION_YEAR, DATE_CREATE) AS admission_year,
    argMax(QUALIFICATION, DATE_CREATE) AS qualification,
    argMax(COURSE, DATE_CREATE) AS course,
    argMax(GROUP_SHORT_TITLE, DATE_CREATE) AS group_short_title,
    argMax(SPECIALIZATION_TITLE, DATE_CREATE) AS specialization_title,
    argMax(TEACHER_FIO, DATE_CREATE) AS teacher_fio,
    argMax(TEACHER_LAST_NAME, DATE_CREATE) AS teacher_last_name,
    argMax(TEACHER_FIRST_NAME, DATE_CREATE) AS teacher_first_name,
    argMax(TEACHER_MIDDLE_NAME, DATE_CREATE) AS teacher_middle_name,
    argMax(TEACHER_WORK_POSITION, DATE_CREATE) AS teacher_work_position,
    argMax(FACULTY, DATE_CREATE) AS faculty,
    argMax(KAFEDRA, DATE_CREATE) AS kafedra,
    argMax(TEACHER_AGE, DATE_CREATE) AS teacher_age,
    argMax(TEACHER_SEX, DATE_CREATE) AS teacher_sex
FROM my_database.attendance_enriched
GROUP BY JOURNAL_ID, STUDENT_ID, LESSON_ID, LESSON_DATE;
```


Агрегация проводилась с помощью argMax по DATE_CREATE, которая оставляет самую позднюю отметку после мёржа.

![alt text](refs/image_7.png)

![alt text](refs/image_8.png)