import json
from config.kafka_config import producer, BATCH_SIZE_KAFKA

def delivery_report(err, msg):
    """Kafka 消息发送回调"""
    if err is not None:
        print(f"Kafka消息发送失败: {err}")
    else:
        print(f"Kafka消息已发送: {msg.key()} -> {msg.value()}")

def send_to_kafka(df, topic: str, columns: list, production_line: str, cameraid:int,batch_size=BATCH_SIZE_KAFKA):
    """批量发送 Kafka 消息（confluent_kafka 版本）"""
    if df.empty:
        return

    key_bytes = f'{production_line}_{cameraid}'.encode("utf-8")
    records = df[columns].to_dict(orient="records")

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        for msg in batch:
            try:
                producer.produce(
                    topic=topic,
                    key=key_bytes,
                    value=json.dumps(msg),
                    callback=delivery_report
                )
            except Exception as e:
                print(f"[Kafka] 发送消息异常: {e}")

        # 批量 flush 一次
        producer.flush()

    print(f"[Kafka] {len(records)} messages sent → {topic}")
