

# config/kafka_config.py
from confluent_kafka import Producer
from config.config_meta import META_COLUMNS

# ---------------- Kafka 配置 ----------------
# ---------------- Kafka ----------------
KAFKA_TOPIC = "rtsp_frames"
KAFKA_SERVERS = "kafka:29092" #"10.161.81.13:8010"#"kafka:29092"  # 对应 INTERNAL listener##"10.161.81.13:8010/kafka/"#"10.161.81.13:9092"
BATCH_SIZE_KAFKA = 50

producer_conf = {
    'bootstrap.servers': KAFKA_SERVERS,
    #'broker.address.family': 'v4',
    # 避免 Kafka 返回内部 host
    #'client.dns.lookup': 'use_all_dns_ips',
}

producer = Producer(producer_conf)


# Kafka topic 配置
kafka_var = {
    "ehs": {
        "topic": "ehs_photo",
        "columns": [
            META_COLUMNS['object_name'],
            META_COLUMNS['product_name'],
            META_COLUMNS['linespeed_spec'],
            META_COLUMNS['linespeed_real'],
            META_COLUMNS['photo2check_bool'],
            META_COLUMNS['photo2minio_status'],
            META_COLUMNS['production_line'],
            META_COLUMNS['cameraid'],
            META_COLUMNS['productnameid'],


            # META['photo2detect']
        ]
    },
    # 可扩展其他 topic
}

