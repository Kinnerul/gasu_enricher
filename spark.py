from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from elasticsearch import Elasticsearch
import os
from datetime import datetime
from pyspark.sql import Row
import logging
import time

# Логирование
logging.basicConfig(level=logging.INFO, filename='attendance_enrichment.log', format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Определяем схемы
attendance_raw_schema = StructType([
    StructField("ID", StringType(), True),
    StructField("JOURNAL_ID", IntegerType(), True),
    StructField("STUDENT_ID", IntegerType(), True),
    StructField("LESSON_DATE", StringType(), True),
    StructField("ATTENDANCE_MARK", IntegerType(), True),
    StructField("MARKED_BY", IntegerType(), True),
    StructField("DATE_CREATE", StringType(), True),
    StructField("LESSON_ID", IntegerType(), True),
    StructField("TYPE_MARK", IntegerType(), True),
    StructField("NUMBER_CERT", IntegerType(), True),
    StructField("MARKED_BY_ROLE", IntegerType(), True)
])

student_enrich_schema = StructType([
    StructField("ENRICHED_STUDENT_ID", StringType(), True),
    StructField("STUDENT_FIO", StringType(), True),
    StructField("STUDENT_LAST_NAME", StringType(), True),
    StructField("STUDENT_FIRST_NAME", StringType(), True),
    StructField("STUDENT_MIDDLE_NAME", StringType(), True),
    StructField("STUDENT_AGE", IntegerType(), True),
    StructField("STUDENT_SEX", StringType(), True),
    StructField("ADMISSION_YEAR", IntegerType(), True),
    StructField("QUALIFICATION", StringType(), True),
    StructField("COURSE", IntegerType(), True),
    StructField("GROUP_SHORT_TITLE", StringType(), True),
    StructField("SPECIALIZATION_TITLE", StringType(), True)
])

teacher_enrich_schema = StructType([
    StructField("ENRICHED_MARKED_BY", StringType(), True),
    StructField("TEACHER_FIO", StringType(), True),
    StructField("TEACHER_LAST_NAME", StringType(), True),
    StructField("TEACHER_FIRST_NAME", StringType(), True),
    StructField("TEACHER_MIDDLE_NAME", StringType(), True),
    StructField("TEACHER_WORK_POSITION", StringType(), True),
    StructField("FACULTY", StringType(), True),
    StructField("KAFEDRA", StringType(), True),
    StructField("TEACHER_AGE", IntegerType(), True),
    StructField("TEACHER_SEX", StringType(), True)
])

# Кэширование данных о студентах и преподавателях
student_cache = {}
teacher_cache = {}
last_clear_time = time.time()

# ФФункция расчета возраста
def calculate_age(birthday):
    if birthday is None:
        return 0
    try:
        today = datetime.today()
        return today.year - birthday.year - ((today.month, today.day) < (birthday.month, birthday.day))
    except Exception as e:
        logger.error(f"Error calculating age: {e}")
        return 0

# Функция для массового извлечения пользовательских данных из Elasticsearch
def fetch_users_bulk(es, index, ids, field, cache):
    if not ids:
        logger.info(f"No IDs to fetch for {field}")
        return {}
    try:
        # Получить данные из кэша
        user_data = {id_: cache.get(id_, None) for id_ in ids}
        # Нахождение потерянных ID
        missing_ids = [id_ for id_ in ids if user_data[id_] is None]
        if missing_ids:
            query = {
                "query": {
                    "terms": {field: missing_ids}
                },
                "size": 10000  
            }
            logger.info(f"Executing Elasticsearch query for {field} with {len(missing_ids)} missing IDs")
            res = es.search(index=index, body=query)
            for hit in res['hits']['hits']:
                source = hit['_source']
                personal = source.get('PERSONAL', {})
                full_name = personal.get('fullname', '')
                last_name = personal.get('lastname', '')
                first_name = personal.get('name', '')
                middle_name = personal.get('surname', '')
                user_type = personal.get('type', '')
                work_position = personal.get('WORK_POSITION', '')
                bitrix_user_id = personal.get('BITRIX_USER_ID', '')
                id_decanat = personal.get('ID_DECANAT', 0)
                birthdate_str = personal.get('birthdate', '')
                if birthdate_str:
                    try:
                        birthdate = datetime.strptime(birthdate_str, '%Y-%m-%dT%H:%M:%SZ')
                    except ValueError:
                        birthdate = None
                else:
                    birthdate = None
                sex = personal.get('sex', '')
                age = calculate_age(birthdate) if birthdate else 0
                user = {
                    'FullName': full_name,
                    'LastName': last_name,
                    'FirstName': first_name,
                    'MiddleName': middle_name,
                    'Type': user_type,
                    'WorkPosition': work_position,
                    'Age': age,
                    'Sex': sex,
                }
                if user_type == 'student' and 'EDUCATION' in source:
                    education = source['EDUCATION'][0] if isinstance(source.get('EDUCATION'), list) and len(source['EDUCATION']) > 0 else {}
                    user['Group'] = education.get('department', {}).get('short_title', '')
                    user['Speciality'] = education.get('specialization', {}).get('title', '')
                    admission_year = education.get('edu_year', '')
                    user['AdmissionYear'] = int(admission_year) if admission_year.isdigit() else 0
                    user['Qualifation'] = education.get('edu_qualification', '')
                    course = education.get('edu_course', '')
                    user['Course'] = int(course) if course.isdigit() else 0
                elif user_type == 'employee' and 'WORK' in source:
                    work = source['WORK'][0] if isinstance(source.get('WORK'), list) and len(source['WORK']) > 0 else {}
                    user['WorkPosition'] = work.get('job_title', '')
                    department = work.get('department', {})
                    user['Faculty'] = department.get('FACULTY_NAME_DECANAT', '')
                    user['Kafedra'] = department.get('title', '')
                if field == 'PERSONAL.BITRIX_USER_ID':
                    key = bitrix_user_id
                elif field == 'PERSONAL.ID_DECANAT':
                    key = str(id_decanat)
                else:
                    key = None
                if key:
                    cache[key] = user
                    user_data[key] = user
            logger.info(f"Retrieved {len(res['hits']['hits'])} new users from Elasticsearch for {field}")
            # Логируем потерянные ID
            still_missing = [id_ for id_ in missing_ids if id_ not in user_data or user_data[id_] is None]
            if still_missing:
                logger.warning(f"Missing {len(still_missing)} IDs for {field} after query: {list(still_missing)[:10]}...")
        user_data = {k: v for k, v in user_data.items() if v is not None}
        logger.info(f"Total retrieved {len(user_data)} users for {field} (including cache)")
        return user_data
    except Exception as e:
        logger.error(f"Error fetching users from Elasticsearch for field {field}: {e}")
        return {}

# Функция Foreach для обогащения
def foreach_batch_function(batch_df, batch_id):
    global student_cache, teacher_cache, last_clear_time
    try:
        # Clear cache every hour
        current_time = time.time()
        if current_time - last_clear_time > 3600:
            student_cache.clear()
            teacher_cache.clear()
            spark.catalog.clearCache()
            last_clear_time = current_time
            logger.info(f"Cleared caches at {datetime.fromtimestamp(current_time)}")

        logger.info(f"Starting batch {batch_id} processing")
        # Проверка на дубликаты
        input_count = batch_df.count()
        duplicate_check = batch_df.groupBy("ID").count().filter("count > 1")
        if duplicate_check.count() > 0:
            logger.warning(f"Duplicates found in input batch {batch_id}:")
            duplicate_check.show()
        logger.info(f"Input batch {batch_id} has {input_count} rows")

        # Извлечение уникальных идентификаторов студентов
        student_ids = [str(row.STUDENT_ID) for row in batch_df.select("STUDENT_ID").distinct().collect() if row.STUDENT_ID is not None]
        # Извлечение уникальных идентификаторов преподавателей
        teacher_df = batch_df.filter(~col("MARKED_BY_ROLE").isin([0,1]))
        teacher_ids = [str(row.MARKED_BY) for row in teacher_df.select("MARKED_BY").distinct().collect() if row.MARKED_BY is not None]
        logger.info(f"Batch {batch_id}: {len(student_ids)} student IDs, {len(teacher_ids)} teacher IDs")


        if input_count > 0:
            logger.info(f"Sample input data for batch {batch_id}:")
            sample_rows = batch_df.limit(2).collect()
            for row in sample_rows:
                logger.info(f"Input row: {row.asDict()}")

        student_data = fetch_users_bulk(es, "test", student_ids, "PERSONAL.BITRIX_USER_ID", student_cache)
        logger.info(f"Retrieved {len(student_data)} student records")


        teacher_data = fetch_users_bulk(es, "test", teacher_ids, "PERSONAL.ID_DECANAT", teacher_cache)
        logger.info(f"Retrieved {len(teacher_data)} teacher records")

        # Создаем датафрейм студента
        student_rows = [Row(
            ENRICHED_STUDENT_ID=k,
            STUDENT_FIO=v.get('FullName', ''),
            STUDENT_LAST_NAME=v.get('LastName', ''),
            STUDENT_FIRST_NAME=v.get('FirstName', ''),
            STUDENT_MIDDLE_NAME=v.get('MiddleName', ''),
            STUDENT_AGE=v.get('Age', 0),
            STUDENT_SEX=v.get('Sex', ''),
            ADMISSION_YEAR=v.get('AdmissionYear', 0),
            QUALIFICATION=v.get('Qualifation', ''),
            COURSE=v.get('Course', 0),
            GROUP_SHORT_TITLE=v.get('Group', ''),
            SPECIALIZATION_TITLE=v.get('Speciality', '')
        ) for k, v in student_data.items()]
        student_enrich_df = spark.createDataFrame(student_rows, student_enrich_schema) if student_rows else spark.createDataFrame([], student_enrich_schema)
        logger.info(f"Created student_enrich_df with {student_enrich_df.count()} rows")

        # Создаем датафрейм преподавателя
        teacher_rows = [Row(
            ENRICHED_MARKED_BY=k,
            TEACHER_FIO=v.get('FullName', ''),
            TEACHER_LAST_NAME=v.get('LastName', ''),
            TEACHER_FIRST_NAME=v.get('FirstName', ''),
            TEACHER_MIDDLE_NAME=v.get('MiddleName', ''),
            TEACHER_WORK_POSITION=v.get('WorkPosition', ''),
            FACULTY=v.get('Faculty', ''),
            KAFEDRA=v.get('Kafedra', ''),
            TEACHER_AGE=v.get('Age', 0),
            TEACHER_SEX=v.get('Sex', '')
        ) for k, v in teacher_data.items()]
        teacher_enrich_df = spark.createDataFrame(teacher_rows, teacher_enrich_schema) if teacher_rows else spark.createDataFrame([], teacher_enrich_schema)
        logger.info(f"Created teacher_enrich_df with {teacher_enrich_df.count()} rows")

        # Джоиним запись
        enriched_df = batch_df.join(
            student_enrich_df,
            batch_df.STUDENT_ID.cast("string") == student_enrich_df.ENRICHED_STUDENT_ID,
            how="left"
        )
        enriched_df = enriched_df.join(
            teacher_enrich_df,
            enriched_df.MARKED_BY.cast("string") == teacher_enrich_df.ENRICHED_MARKED_BY,
            how="left"
        )
        logger.info(f"Enriched DataFrame before deduplication has {enriched_df.count()} rows")

        # Дедупликация по ID
        enriched_df = enriched_df.dropDuplicates(["ID"])
        logger.info(f"Enriched DataFrame after deduplication has {enriched_df.count()} rows")

        duplicate_check_after = enriched_df.groupBy("ID").count().filter("count > 1")
        if duplicate_check_after.count() > 0:
            logger.warning(f"Duplicates found after enrichment in batch {batch_id}:")
            duplicate_check_after.show()

        if enriched_df.count() > 0:
            logger.info(f"Sample enriched data for batch {batch_id}:")
            sample_enriched = enriched_df.limit(2).collect()
            for row in sample_enriched:
                logger.info(f"Enriched row: {row.asDict()}")

        output_columns = [
            "ID", "JOURNAL_ID", "STUDENT_ID", "LESSON_DATE", "ATTENDANCE_MARK", "MARKED_BY",
            "DATE_CREATE", "LESSON_ID", "TYPE_MARK", "NUMBER_CERT", "MARKED_BY_ROLE",
            "STUDENT_FIO", "STUDENT_LAST_NAME", "STUDENT_FIRST_NAME", "STUDENT_MIDDLE_NAME",
            "STUDENT_AGE", "STUDENT_SEX", "ADMISSION_YEAR", "QUALIFICATION", "COURSE",
            "GROUP_SHORT_TITLE", "SPECIALIZATION_TITLE",
            "TEACHER_FIO", "TEACHER_LAST_NAME", "TEACHER_FIRST_NAME", "TEACHER_MIDDLE_NAME",
            "TEACHER_WORK_POSITION", "FACULTY", "KAFEDRA", "TEACHER_AGE", "TEACHER_SEX"
        ]
        logger.info(f"Writing enriched data to Kafka topic 'attendance_enriched'")
        enriched_df.select(col("ID").alias("key"),
                          to_json(struct([col(c) for c in output_columns])).alias("value")) \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "192.168.128.167:9092") \
            .option("topic", "attendance_enriched") \
            .option("kafka.batch.size", "16384") \
            .option("kafka.linger.ms", "5") \
            .option("kafka.compression.type", "snappy") \
            .option("kafka.failOnDataLoss", "true") \
            .option("kafka.enable.idempotence", "true") \
            .option("kafka.acks", "all") \
            .option("kafka.retries", "2147483647") \
            .save()
        logger.info(f"Successfully wrote batch {batch_id} to Kafka")
        # В конце foreach_batch_function
        written_rows = enriched_df.count()
        logger.info(f"Batch {batch_id} wrote {written_rows} rows to Kafka topic 'attendance_enriched'")
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")
        raise

try:
    spark = SparkSession.builder \
        .appName("AttendanceEnrichment") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()

    # Set up Elasticsearch
    password = os.getenv("ES_PASSWORD")
    if password is None:
        raise ValueError("Environment variable ES_PASSWORD is not set")
    
    ca_cert_path = "/etc/elasticsearch/certs/http_ca.crt"
    if not os.path.exists(ca_cert_path):
        raise FileNotFoundError(f"CA certificate file not found at {ca_cert_path}")
    
    logger.info(f"Using CA certificate at {ca_cert_path}")
    es = Elasticsearch(
        ["https://192.168.128.165:9200"],
        basic_auth=("elastic", password),
        ca_certs=ca_cert_path,
        verify_certs=True
    )

    # Test Elasticsearch connection
    if not es.ping():
        raise ValueError("Failed to connect to Elasticsearch")
    logger.info("Successfully connected to Elasticsearch")

    # Чтение из Кафки
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.128.167:9092") \
        .option("subscribe", "attendance_events") \
        .option("startingOffsets", "earliest") \
        .option("kafka.group.id", "attendance-consumer-group") \
        .load()

    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(from_json(col("value"), attendance_raw_schema).alias("data")).select("data.*")
    df = df.filter(col("ID").isNotNull())
    df = df.dropDuplicates(["ID"])

    query = df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
except Exception as e:
    logger.error(f"Fatal error in main: {e}")
    raise
finally:
    spark.stop()
