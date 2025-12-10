import boto3
import datetime
from botocore.exceptions import ClientError
from config.config_meta import MINIO_ENDPOINT, ACCESS_KEY,SECRET_KEY,BUCKET_NAME
import numpy as np
import cv2

# 假设您的 YOLO 应用配置
#MINIO_ENDPOINT = 'http://minio:9000'#'http://10.161.67.41:9000'##'http://127.0.0.1:9000'# 'http://minio:9000'#'http://127.0.0.1:9000'
#ACCESS_KEY = 'admin_minio'
#SECRET_KEY = 'admin_minio'
#BUCKET_NAME = 'ehskunshan'

# 1. 初始化 MinIO 客户端 (使用 boto3，MinIO 兼容 S3 API)
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=boto3.session.Config(proxies={})  # 不使用代理
    # 如果使用 https，需要设置 verify=True
)



def _generate_save_path_minio(production_line,save_dir_relative='rawpic',extension=".BMP"):


    now_utc = datetime.datetime.now(datetime.timezone.utc)
    
    # 2. 格式化日期和时间字符串（基于 UTC 时间）
    date_path = now_utc.strftime('%Y%m%d') # UTC 的 年/月/日
    time_str = now_utc.strftime('%Y%m%d%H%M%S') # UTC 的 年月日时分秒
  
   
    object_name = f"{save_dir_relative}/{production_line}/{date_path}/image_{time_str}{extension}"

    
    return object_name


def store_yolo_image(frame_data, object_name,ContentType='image/bmp',BUCKET_NAME=BUCKET_NAME):
    """
    存储YOLO诊断出的图片
    :param frame_data: 图片的二进制数据 (如 OpenCV 编码后的数据)
    :param camera_id: 摄像头ID
    :param detection_tag: 检测到的主要标签 (如 Human, Car, Anomaly)
    """
    #now = datetime.now()
    #date_str = now.strftime('%Y-%m-%d')
    #time_str = now.strftime('%H%M%S_%f')[:-3]  # 包含毫秒
    
    # 3. 构造 Key (模拟路径)
    #object_name = f"{camera_id}/{date_str}/{time_str}/{detection_tag}.jpg"
    
    # 4. 上传图片
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except ClientError:
        print(f"Bucket {BUCKET_NAME} 不存在，创建中...")
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket {BUCKET_NAME} 创建完成")
    try:
        # Fileobj 可以是文件对象，也可以是 BytesIO 对象
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=object_name,
            Body=frame_data, 
            ContentType=ContentType
        )
        print(f"✅ 图片成功上传到 MinIO. Key: {object_name}")
        return True
    except Exception as e:
        print(f"❌ MinIO 上传失败: {e}")
        return False


def savepic_2minio_1pic(result, BUCKET_NAME=BUCKET_NAME, ContentType='image/bmp', save_dir_relative='rawpic',production_line="C11", extension=".BMP"):
    """
    处理结果、绘制边界框并将图片保存到本地，路径包含日期层级。
    
    Args:
        results (list): 模型预测结果列表。
        save_dir (str): 图片保存的基础目录。
        custom_colors (dict): 用于绘图的自定义颜色。
        line_id (str): 用于生成文件名的产线标识符。
        extension (str): 目标保存的文件扩展名。
        
    Returns:
        tuple: (results, p_name_list, save_dir_list)
    """
 
    object_name = _generate_save_path_minio(save_dir_relative=save_dir_relative,
                                                    production_line=production_line,
                                                    extension=extension
                                                    )


    save_result = store_yolo_image(frame_data=result, object_name=object_name,ContentType=ContentType,BUCKET_NAME=BUCKET_NAME)

    return  object_name,save_result



def load_yolo_image(bucket, object_name):
    """
    从 MinIO 拉取图片，并转换为 OpenCV 的 numpy 数组。
    """
    try:
        response = s3.get_object(Bucket=bucket, Key=object_name)
        binary = response["Body"].read()
    except ClientError as e:
        print(f"❌ 读取失败：{e}")
        return None

    # 转 numpy 数组
    np_arr = np.frombuffer(binary, dtype=np.uint8)

    # 解码成 OpenCV 图片（bmp/jpg/png 都支持）
    img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    if img is None:
        print("❌ 解码失败，二进制不是有效图片格式")
        return None

    return img