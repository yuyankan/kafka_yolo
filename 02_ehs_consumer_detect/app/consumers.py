# app/consumers.py
from core.kafka_agent import KafkaAgent
from config.kafka_config import KAFKA_SERVERS,kafka_var
import pandas as pd
from core.detection_util import *
from app.workflow import process_data_custom
from core.detection_util import load_model
from config.config_meta import model_path



def process_data_2df(topic_name, partition_id, data):
    """示例业务函数：处理 trigger 消息"""
    if not isinstance(data, list):
        data = [data]
    df_data = pd.DataFrame(data)
    df_data['topic_name'] = topic_name
    df_data['partition_id'] = partition_id
    print(f"[TRIGGER] topic_name:{topic_name}, partition_id:{partition_id}, 处理数据:")
    print('data:', data)
    print('df_data', df_data.head())
    # TODO: 调用 PLC / DB / Kafka Detail 逻辑

    return df_data



def process_data_work(topic_name, partition_id, data,model, device):
    df_data = process_data_2df(topic_name=topic_name, 
                               partition_id=partition_id, 
                               data=data)

    # dealing with
    process_data_custom(df_data=df_data, model=model, device=device)
    




# 1. 修改 create_trigger_consumer
def create_trigger_consumer(production_line, cameraid):
    # 假设这里是 KafkaAgent 的初始化
    # model_path, custom_colors 等配置应该从 config 导入
    
    # **预加载模型 (只执行一次)**
    model, device = load_model(model_path)
    
    # 假设 KafkaAgent 实例可以存储这些属性
    # 实际操作可能需要在 KafkaAgent 类中添加属性来存储 model 和 device
    group_id = f"ech_consumer_{production_line}_{cameraid}" # corresponding to key for producer

    agent = KafkaAgent(
        topics=kafka_var['ehs']['topic'],
        bootstrap_servers=KAFKA_SERVERS,
        group_id=group_id,
        model = model,
        device = device
        #offset_file="offsets.json",
        #production_line=production_line
    )
    
    # 临时解决：如果 KafkaAgent 不支持，可以返回一个包含模型和agent的包装对象
    # 更优雅的方式是修改 KafkaAgent 类的 __init__ 方法，让它存储 self.model, self.device

    return agent