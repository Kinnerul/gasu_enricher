public static function setAttendance($attendaceArray)
    {
        /**
         * СИСТЕМА РОЛЕЙ MARKED_BY_ROLE:
         * 3 - Деканат (высший приоритет) - только для значений посещаемости 2 или 3
         *     И только если пользователь является сотрудником деканата
         * 2 - Преподаватель - для всех остальных случаев (значения 0, 1, 4, 5)
         * 1 - Староста (ответственный студент) - может отмечать других студентов в своей группе
         * 0 - Обычный студент - может отмечать только себя
         *
         * ИЕРАРХИЯ ПРАВ: 3 > 2 > 1 > 0
         */
        PushPull\Manager::sendDataUpdate(13096, "schedule", "hellow_back");
        // 1. ВАЛИДАЦИЯ ВХОДНЫХ ДАННЫХ
        if (empty($attendaceArray)) {
            return "Массив пуст";
        }

        // Проверяем обязательные поля
        $requiredFields = ['journalId', 'userId', 'lessonId', 'attendance'];
        foreach ($requiredFields as $field) {
            if (!isset($attendaceArray[$field])) {
                return "Отсутствует обязательное поле: $field";
            }
        }

        // Валидация типов данных (защита от подделки)
        if (!is_numeric($attendaceArray['journalId']) || $attendaceArray['journalId'] <= 0) {
            return "Неверный ID журнала";
        }
        if (!is_numeric($attendaceArray['userId']) || $attendaceArray['userId'] <= 0) {
            return "Неверный ID пользователя";
        }
        if (!is_numeric($attendaceArray['lessonId']) || $attendaceArray['lessonId'] <= 0) {
            return "Неверный ID занятия";
        }

        // Валидация attendance (подозрительное поле)
        $validAttendanceValues = [0, 1,2,3]; // только 0 (отсутствие) или 1 (присутствие)
        if (!in_array((int)$attendaceArray['attendance'], $validAttendanceValues, true)) {
            return "Неверное значение посещаемости";
        }

        // Поле note не используется в системе
        // if (isset($attendaceArray['note'])) {
        //     if (!is_numeric($attendaceArray['note']) || $attendaceArray['note'] < 0) {
        //         return "Неверное значение заметки";
        //     }
        // }

        $lessonsTime = array(
            '1' => array('start' => '09:00', 'end' => '10:30'),
            '2' => array('start' => '10:45', 'end' => '12:15'),
            '3' => array('start' => '12:30', 'end' => '14:00'),
            '4' => array('start' => '15:00', 'end' => '16:30'),
            '5' => array('start' => '16:45', 'end' => '18:15'),
            '6' => array('start' => '18:30', 'end' => '20:00'),
            '7' => array('start' => '20:15', 'end' => '21:45'),
            '8' => array('start' => '22:00', 'end' => '23:30'),
        );

        // 2. ПОЛУЧЕНИЕ ИНФОРМАЦИИ О ТЕКУЩЕМ ПОЛЬЗОВАТЕЛЕ
        $currentUser = CurrentUser::getInstance();
        $permission =new Permission($currentUser);
        $userRole = $currentUser->getRole(); // 'employee' или 'student'
        $userId = $currentUser->getPersonalBitrixId();

        // 3. ПРОВЕРКА ДОСТУПА В ЗАВИСИМОСТИ ОТ РОЛИ
        if ($userRole === 'employee') {
            // ЛОГИКА ДЛЯ ПРЕПОДАВАТЕЛЯ
            $prepodDekantId = $currentUser->getIdDecanat();
            if (!$prepodDekantId) {
                return "Не удалось определить ID преподавателя";
            }

            // Проверяем доступ преподавателя к журналу
            $fbPermissions = $permission->getFBPermission();
            $hasJournalAccess = false;

            // Проверяем доступ через FBPermission
            if (isset($fbPermissions['FB']['journal'][$attendaceArray['journalId']])) {
                $hasJournalAccess = true;
            }

            // Проверяем является ли преподаватель ответственным за журнал
            $responsibleCheck = JournalResponsibleTeachersTable::getList([
                'filter' => [
                    'JOURNAL_ID' => (int)$attendaceArray['journalId'],
                    'RESPONSIBLE_ID' => $prepodDekantId
                ],
                'select' => ['ID'],
                'limit' => 1
            ]);

            if ($responsibleCheck->fetch()) {
                $hasJournalAccess = true;
            }

            if (!$hasJournalAccess) {
                return "Нет доступа к данному журналу";
            }

        } elseif ($userRole === 'student') {
            // ЛОГИКА ДЛЯ СТУДЕНТА

            // Получаем доступные журналы из FBPermission["FB"]["journal"] для старост
            $fbPermissions = $permission->getFBPermission();
            $availableJournalIds = [];
            if (isset($fbPermissions['FB']['journal'])) {
                $availableJournalIds = array_keys($fbPermissions['FB']['journal']);
            }

            // Получаем журналы для студентов (связанные с их группой)
            // Permission автоматически загружает журналы студента через loadStudentJournalsByGroup()
            $studentJournalIds = $permission->getAvailableJournalIds();

            // Получаем ответственных студентов (старост) для журнала
            $responsibleStudents = [];
            $resSID = JournalResponsibleStudentTable::getList([
                'filter' => ['JOURNAL_ID' => (int)$attendaceArray['journalId']],
                'select' => ["RESPONSIBLE_ID"],
            ]);

            while ($res = $resSID->fetch()) {
                $responsibleStudents[] = $res["RESPONSIBLE_ID"];
            }

            $isResponsibleStudent = in_array($userId, $responsibleStudents);
            $hasJournalInFB = in_array($attendaceArray['journalId'], $availableJournalIds);
            $hasStudentAccess = in_array($attendaceArray['journalId'], $studentJournalIds);

            // Проверяем права студента
            if ($isResponsibleStudent || $hasJournalInFB) {
                // Студент-староста: может отмечать всех в журнале
                if (!$hasStudentAccess && !$hasJournalInFB) {
                    return "Нет доступа к данному журналу";
                }
            } else {
                // Обычный студент: может отмечать только себя
                if ($userId != $attendaceArray['userId']) {
                    return "Можете отмечать только себя";
                }
                if (!$hasStudentAccess) {
                    return "Нет доступа к данному журналу";
                }
            }

        } else {
            return "Неопределенная роль пользователя";
        }

        // 4. ПРОВЕРКА ИНФОРМАЦИИ О ЗАНЯТИИ
        $lessonInfo = LessonsTable::getList([
            'filter' => ['ID' => (int)$attendaceArray['lessonId']],
            'select' => ['DATE', 'TYPE_ID', 'NUMBER', 'BLOCKED'],
            'limit' => 1
        ]);

        $lesInfo = $lessonInfo->fetch();
        if (!$lesInfo) {
            return "Занятие не найдено";
        }

        // Проверяем, не заблокировано ли занятие
        if ($lesInfo["BLOCKED"] == 1) {
            return "Занятие заблокировано для редактирования";
        }

        // Валидация даты занятия (подозрительное поле)
        $lessonDate = date('Y-m-d', strtotime($lesInfo["DATE"]));
        if (isset($attendaceArray['lessonDate'])) {
            $providedDate = date('Y-m-d', strtotime($attendaceArray['lessonDate']));
            if ($providedDate !== $lessonDate) {
                return "Переданная дата не соответствует дате занятия";
            }
        }

        // 5. ПРОВЕРКА СУЩЕСТВУЮЩИХ ОТМЕТОК
        $clickconnect = new ClickHouse();

        // Используем параметризированный запрос для защиты от SQL инъекций
        $sql = "SELECT ID, MARKED_BY, ATTENDANCE_MARK, MARKED_BY_ROLE, DATE_CREATE 
                FROM attendance_test_enriched 
                WHERE JOURNAL_ID = ? 
                  AND STUDENT_ID = ? 
                  AND LESSON_ID = ? 
                  AND LESSON_DATE = ? 
                  AND TYPE_MARK = 0
                ORDER BY DATE_CREATE DESC 
                LIMIT 1";

        // Для ClickHouse используем sprintf с проверенными данными
        $safeSql = sprintf(
            "SELECT ID, MARKED_BY, ATTENDANCE_MARK, MARKED_BY_ROLE, DATE_CREATE 
             FROM attendance_test_enriched 
             WHERE JOURNAL_ID = %d 
               AND STUDENT_ID = %d 
               AND LESSON_ID = %d 
               AND LESSON_DATE = '%s' 
               AND TYPE_MARK = 0
             ORDER BY DATE_CREATE DESC 
             LIMIT 1",
            (int)$attendaceArray['journalId'],
            (int)$attendaceArray['userId'],
            (int)$attendaceArray['lessonId'],
            $lessonDate
        );

        $existingAttendance = $clickconnect->DB->select($safeSql);
        $existingRecord = null;
        foreach ($existingAttendance as $record) {
            $existingRecord = $record;
            break;
        }

        // 6. ПРОВЕРКА ВРЕМЕНИ ДЛЯ СТУДЕНТОВ
        if ($userRole === 'student') {
            // Проверяем время пары для студентов
            if (!isset($lessonsTime[$lesInfo["NUMBER"]])) {
                return "Неверный номер пары";
            }

            $currentTime = new DateTime();
            $pairStart = DateTime::createFromFormat('H:i', $lessonsTime[$lesInfo["NUMBER"]]['start']);
            $pairEnd = DateTime::createFromFormat('H:i', $lessonsTime[$lesInfo["NUMBER"]]['end']);

            if ($currentTime <= $pairStart || $currentTime >= $pairEnd) {
                return "Отметка вне времени пары";
            }
        }
        // 7. ПОДГОТОВКА ДАННЫХ ДЛЯ ЗАПИСИ
        $dateTime = new DateTime();
        $formattedDateTime = $dateTime->format('Y-m-d H:i:s.u');
        $uuid = Uuid::uuid4()->toString();

        // Определяем MARKED_BY и MARKED_BY_ROLE в зависимости от роли и прав
        if ($userRole === 'employee') {
            $markedBy = $prepodDekantId;

            // Проверяем является ли пользователь сотрудником деканата И значение посещаемости 2 или 3
            $availableDecanatIds = $permission->getAvailableDecanatIds();
            $attendanceMark = (int)$attendaceArray['attendance'];

            if (!empty($availableDecanatIds) && ($attendanceMark === 2 || $attendanceMark === 3)) {
                $markedByRole = 3; // Деканат - только для посещаемости 2 или 3
            } else {
                $markedByRole = 2; // Преподаватель - для всех остальных случаев
            }
        } else {
            // Для студентов проверяем является ли он старостой
            $markedBy = $userId;

            $responsibleStudents = [];
            $resSID = JournalResponsibleStudentTable::getList([
                'filter' => ['JOURNAL_ID' => (int)$attendaceArray['journalId']],
                'select' => ["RESPONSIBLE_ID"],
            ]);

            while ($res = $resSID->fetch()) {
                $responsibleStudents[] = $res[" Polesponsible_ID"];
            }

            $isResponsibleStudent = in_array($userId, $responsibleStudents);
            $availableJournalIds = array_keys($permission->getFBPermission()['FB']['journal'] ?? []);
            $hasJournalInFB = in_array($attendaceArray['journalId'], $availableJournalIds);

            if ($isResponsibleStudent || $hasJournalInFB) {
                $markedByRole = 1; // Староста
            } else {
                $markedByRole = 0; // Обычный студент
            }
        }

        $attendanceData = [
            "JOURNAL_ID" => (int)$attendaceArray['journalId'],
            "STUDENT_ID" => (int)$attendaceArray['userId'],
            "LESSON_ID" => (int)$attendaceArray['lessonId'],
            "LESSON_DATE" => $lessonDate,
            "ATTENDANCE_MARK" => (int)$attendaceArray['attendance'],
            "MARKED_BY" => $markedBy,
            'ID' => $uuid,
            'DATE_CREATE' => $formattedDateTime,
            'TYPE_MARK' => 0,
            "MARKED_BY_ROLE" => $markedByRole
        ];

        // Инициализация Kafka Producer
        $kafkaConfig = new \RdKafka\Conf();
        $kafkaConfig->set('metadata.broker.list', '192.168.128.167:9092'); 
        $kafkaConfig->set('log_level', (string)LOG_DEBUG); // Настройка уровня логов
        $kafkaConfig->set('debug', 'all'); // Для отладки 

        $producer = new \RdKafka\Producer($kafkaConfig);
        $topicName = 'attendance_events'; // Имя топика Kafka
        $topic = $producer->newTopic($topicName);

        // 8. ЛОГИКА ВСТАВКИ С УЧЕТОМ ИЕРАРХИИ ПРАВ
        if (!$existingRecord) {
         /*   // Отметки нет - создаем новую
            $insertResult = $clickconnect->DB->insert('attendance',
                [$attendanceData],
                array_keys($attendanceData)
            );

            if ($insertResult) {
                // Отправка сообщения в Kafka
              */  try {
                    $message = json_encode($attendanceData);
                    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $uuid);
                    $producer->poll(0); // Немедленная отправка
                    $producer->flush(10000); // Ожидание подтверждения (10 секунд)


            } catch (\Exception $e) {
                    // Логируем ошибку Kafka, но не прерываем выполнение
                    error_log("Kafka produce error: " . $e->getMessage());
                }
                return self::createSuccessResponse($attendanceData, $lesInfo);
          /*  } else {
                return "Ошибка при добавлении отметки";
            }*/
        } else {
            // Отметка уже есть - применяем иерархию прав
            if ($userRole === 'employee') {
                // ПРЕПОДАВАТЕЛЬ: может перезаписывать любые отметки
                if ($existingRecord['MARKED_BY'] == $markedBy &&
                    $existingRecord['ATTENDANCE_MARK'] == $attendaceArray['attendance']) {
                    return "Отметка уже поставлена с таким же значением";
                }

                $insertResult = $clickconnect->DB->insert('attendance',
                    [$attendanceData],
                    array_keys($attendanceData)
                );

                if ($insertResult) {
                    // Отправка сообщения в Kafka
                    try {
                        $message = json_encode($attendanceData);
                        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $uuid);
                        $producer->poll(0);
                        $producer->flush(10000);
                    } catch (\Exception $e) {
                        error_log("Kafka produce error: " . $e->getMessage());
                    }

                    if ($existingRecord['MARKED_BY_ROLE'] === 2 || $existingRecord['MARKED_BY_ROLE'] === 3) {
                        if ($markedByRole === 3) {
                            return self::createSuccessResponse($attendanceData, $lesInfo);
                        } else {
                            return self::createSuccessResponse($attendanceData, $lesInfo);
                        }
                    } else {
                        if ($markedByRole === 3) {
                            return self::createSuccessResponse($attendanceData, $lesInfo);
                        } else {
                            return self::createSuccessResponse($attendanceData, $lesInfo);
                        }
                    }
                } else {
                    if ($markedByRole === 3) {
                        return "Ошибка при добавлении отметки деканата";
                    } else {
                        return "Ошибка при добавлении отметки преподавателя";
                    }
                }

            } elseif ($userRole === 'student') {
                // СТУДЕНТ: не может перезаписывать отметки преподавателей/деканата
                if ($existingRecord['MARKED_BY_ROLE'] === 2 || $existingRecord['MARKED_BY_ROLE'] === 3) {
                    return "Нельзя изменить отметку преподавателя";
                }

                // Проверяем права старосты
                $responsibleStudents = [];
                $resSID = JournalResponsibleStudentTable::getList([
                    'filter' => ['JOURNAL_ID' => (int)$attendaceArray['journalId']],
                    'select' => ["RESPONSIBLE_ID"],
                ]);

                while ($res = $resSID->fetch()) {
                    $responsibleStudents[] = $res["RESPONSIBLE_ID"];
                }

                $isResponsibleStudent = in_array($userId, $responsibleStudents);
                $availableJournalIds = array_keys($permission->getFBPermission()['FB']['journal'] ?? []);

                if ($isResponsibleStudent || in_array($attendaceArray['journalId'], $availableJournalIds)) {
                    // СТАРОСТА: может перезаписывать отметки других студентов
                    if ($existingRecord['MARKED_BY'] == $markedBy &&
                        $existingRecord['ATTENDANCE_MARK'] == $attendaceArray['attendance']) {
                        return "Отметка уже поставлена с таким же значением";
                    }

                    $insertResult = $clickconnect->DB->insert('attendance',
                        [$attendanceData],
                        array_keys($attendanceData)
                    );

                    if ($insertResult) {
                        // Отправка сообщения в Kafka
                        try {
                            $message = json_encode($attendanceData);
                            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $uuid);
                            $producer->poll(0);
                            $producer->flush(10000);
                        } catch (\Exception $e) {
                            error_log("Kafka produce error: " . $e->getMessage());
                        }
                        return self::createSuccessResponse($attendanceData, $lesInfo);
                    } else {
                        return "Ошибка при добавлении отметки старосты";
                    }
                } else {
                    // ОБЫЧНЫЙ СТУДЕНТ: не может перезаписывать отметки старосты или преподавателя/деканата
                    if ($existingRecord['MARKED_BY_ROLE'] === 1 && $existingRecord['MARKED_BY'] != $markedBy) {
                        return "Нельзя изменить отметку старосты";
                    }

                    if ($existingRecord['MARKED_BY'] != $markedBy) {
                        return "Нельзя изменить отметку другого пользователя";
                    }

                    if ($existingRecord['ATTENDANCE_MARK'] == $attendaceArray['attendance']) {
                        return "Отметка уже поставлена с таким же значением";
                    }

                    $insertResult = $clickconnect->DB->insert('attendance',
                        [$attendanceData],
                        array_keys($attendanceData)
                    );

                    if ($insertResult) {
                        // Отправка сообщения в Kafka
                        try {
                            $message = json_encode($attendanceData);
                            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $uuid);
                            $producer->poll(0);
                            $producer->flush(10000);

                        } catch (\Exception $e) {
                            error_log("Kafka produce error: " . $e->getMessage());
                        }
                        return self::createSuccessResponse($attendanceData, $lesInfo);
                    } else {
                        return "Ошибка при обновлении отметки студента";
                    }
                }
            }
        }
    }
