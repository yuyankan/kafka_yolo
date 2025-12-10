import json
from confluent_kafka import Consumer, KafkaException, TopicPartition
import sys
import time
import logging
from typing import Callable, Dict, Tuple, List, Any

# 全局配置，请根据您的环境修改
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_CONSUMER_GROUP_ID = "my-confluent-group"

# 设置日志系统
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BatchProcessFun = Callable[[str, int, List[Tuple[int, Any]]], None]
#class ConfluentKafkaAgent:
class KafkaAgent:
    """
    使用 confluent_kafka 库的消费者代理，推荐使用 Kafka/Zookeeper 管理 offset。
    """
    def __init__(self, topics, model, device,group_id,bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, auto_offset_reset='earliest', enable_auto_commit=True):
        if isinstance(topics, str):
            topics = [topics]
        self.topics = topics
        #self.production_line = production_line
        # 将批处理状态定义为实例属性
        self._message_buffer: Dict[Tuple[str, int], List[Tuple[int, Any]]] = {}
        self._batch_start_time: Dict[Tuple[str, int], float] = {}
        # ✅ 存储模型和设备作为实例属性
        self.model = model
        self.device = device
        
        # 1. 配置 Consumer
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,  # 'earliest' 或 'latest'
            'enable.auto.commit': enable_auto_commit,
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 300000, # 消息处理时间过长时设置此项
            # 'default.topic.config': {'auto.offset.reset': 'earliest'} # 可选
        }

        # 2. 初始化 Consumer
        self.consumer = Consumer(conf)
        
        # 3. 订阅 Topic
        self.consumer.subscribe(self.topics)
        print(f"Consumer subscribed to topics: {self.topics}")

    def _handle_message_error(self, msg: Any) -> bool:
        """
        处理 Kafka 消息错误。
        返回 True 表示遇到致命错误需要停止，返回 False 表示可以继续。
        """
        if msg.error():
            error = msg.error()
            if error.fatal():
                logger.error(f"Fatal Consumer error, stopping: {error}")
                return True
            else:
                logger.warning(f"Consumer error (recoverable): {error}")
                return False
        return False

    def _decode_and_parse_message(self, msg: Any) -> Tuple[str, int, int, Any]:
        """
        解码和反序列化消息值，并提取元数据。
        成功返回 (topic, partition, offset, value_obj)，失败抛出异常。
        """
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        
        value_bytes = msg.value()
        if value_bytes is None:
             raise ValueError("Message value is None.")
        
        try:
            value_str = value_bytes.decode('utf-8')
            value = json.loads(value_str)
            return topic, partition, offset, value
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to decode/parse message for {topic}/{partition} @ {offset}. Error: {e}")
            raise # 统一向上抛出，由调用方决定跳过
        except Exception as e:
            logger.error(f"Unknown error processing message for {topic}/{partition} @ {offset}: {e}")
            raise

    # 🚀 优化点：将 flush_batch 改为类方法
    def _flush_batch(self, topic: str, partition: int, process_fun: BatchProcessFun):
        """对特定 Topic-Partition 的消息执行批量处理并清空缓冲区。"""
        key = (topic, partition)
        
        # 直接访问实例属性
        batch_data = self._message_buffer.pop(key, [])
        if not batch_data:
            return
        
        # 清除开始时间记录
        self._batch_start_time.pop(key, None)
        
        # 批量调用业务处理函数
        process_fun(topic, partition, batch_data, self.model, self.device)
        
        logger.debug(f"Flushed batch for {topic}/{partition}. Size: {len(batch_data)}")


    def run(self, process_fun: Callable):
        """
        启动单消息消费循环（代码与上一个优化版本一致）。
        """
        logger.info("Starting single message consumer loop...")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue

                if self._handle_message_error(msg):
                    break
                
                try:
                    topic_name, partition_id, _, value = self._decode_and_parse_message(msg)
                except Exception:
                    continue
                
                process_fun(topic_name, partition_id, [value])
                
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user (KeyboardInterrupt).")
        
        finally:
            logger.info("Closing consumer...")
            self.consumer.close()


    def run_batch(self, process_fun: BatchProcessFun, batch_size: int = 100, batch_timeout_ms: int = 3000):
        """
        启动批量消费循环。
        """
        # 清空状态，确保 run_batch 的每次调用都是新的批处理周期
        self._message_buffer.clear()
        self._batch_start_time.clear()

        timeout_seconds = batch_timeout_ms / 1000.0
        
        logger.info("Starting batch consumer loop...")
        try:
            while True:
                msg = self.consumer.poll(0.1) 
                
                # --- A. 检查并处理收到的消息 ---
                if msg is not None:
                    
                    if self._handle_message_error(msg):
                        break
                    
                    # 消息处理 (解码和反序列化)
                    try:
                        topic, partition, offset, value = self._decode_and_parse_message(msg)
                    except Exception:
                        continue 

                    key = (topic, partition)

                    # 2. 消息入缓冲区
                    if key not in self._message_buffer:
                        self._message_buffer[key] = []
                        self._batch_start_time[key] = time.time()
                    
                    #self._message_buffer[key].append((offset, value))
                    self._message_buffer[key].append(value)
                    
                    # 3. 检查是否达到批次大小
                    if len(self._message_buffer[key]) >= batch_size:
                        # 调用类方法，传入 process_fun
                        self._flush_batch(topic, partition, process_fun)
                
                # --- B. 检查并处理批次超时 ---
                
                keys_to_check = list(self._batch_start_time.keys())
                current_time = time.time()
                
                for topic, partition in keys_to_check:
                    key = (topic, partition)
                    start_time = self._batch_start_time.get(key)
                    
                    # 检查缓冲区是否存在（确保计时器和数据匹配）且计时有效
                    if key in self._message_buffer and start_time is not None:
                        if current_time - start_time >= timeout_seconds:
                            # 调用类方法，传入 process_fun
                            self._flush_batch(topic, partition, process_fun)
                            
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user (KeyboardInterrupt).")
        
        finally:
            # 5. 关闭 Consumer，退出前刷新所有剩余缓冲区中的数据
            logger.info("Flushing remaining batches before closing...")
            # 遍历 message_buffer 的键，确保清空所有数据
            for topic, partition in list(self._message_buffer.keys()):
                # 传入 process_fun
                self._flush_batch(topic, partition, process_fun) 
                
            logger.info("Closing consumer...")
            self.consumer.close()

# --- 示例业务处理函数 ---
def process_data(topic_name,partition_id,data):
    """一个简单的处理函数，打印接收到的数据"""
    print(f"Received data: {data}")
    # time.sleep(0.1) # 模拟处理耗时

# --- 运行示例 ---
# if __name__ == '__main__':
#     # 示例：使用手动提交 offset
#     agent = ConfluentKafkaAgent(
#         topics=["your-topic-name"],
#         group_id=KAFKA_CONSUMER_GROUP_ID,
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#         enable_auto_commit=True # 推荐使用自动提交，除非您需要“恰好一次”语义
#     )
#     agent.run(process_data)