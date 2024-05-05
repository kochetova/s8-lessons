USER_HOST = '178.154.207.86' # укажите свой хост
TOPIC_NAME = 'student.topic.cohort21.ddkochetova' # укажите название топика student.topic.cohort<номер когорты>.<username>

#docker run -it --network=rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 edenhill/kcat:1.7.1 -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username=de-student -X sasl.password=ltcneltyn -X ssl.ca.location=/lessons/CA.pem

#kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username="de-student" -X sasl.password="ltcneltyn" -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt -t base -K: -P -l /lessons/your-folder/old-yet-gold.txt