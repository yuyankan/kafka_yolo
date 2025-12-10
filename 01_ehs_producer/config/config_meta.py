# config.py

import os

# ---------------- RTSP ----------------
nvr_ip_address = "147.121.119.88"  # 替换为您的NVR的IP地址
rtsp_port = "554"                 # RTSP端口，通常是554
username = "lgmpublic"                # 替换为您的NVR用户名
PASSWORD = "Avery@1234"    # !!! 替换为您的NVR密码 !!!

# 您想访问的摄像机在NVR上的通道号 (例如，NVR上的D1通道通常是1)
# 假设您想访问的sensor (摄像机) 在NVR上被配置为通道1
channel_id = 4

# 选择码流类型: 1为主码流, 2为子码流
# 例如，通道1的主码流用 101, 子码流用 102
# 通道2的主码流用 201, 子码流用 202, 以此类推
stream_type_id_main = "01" # 主码流
stream_type_id_sub = "02"  # 子码流

rtsp_channel_identifier = str(channel_id) + stream_type_id_main
RTSP_URL = f"rtsp://{username}:{PASSWORD}@{nvr_ip_address}:{rtsp_port}/Streaming/Channels/{rtsp_channel_identifier}"

#freq
FRAME_INTERVAL = 3  # 秒

# ---------------- LOGGING ----------------
SCREENSHOT_DIR = "screenshots_20251201"
LOG_FILE = "alarm_log2.txt"
MINIO_SAVE_DIR = "rawpic"
PRODUCTION_LINE = "C11"

os.makedirs(SCREENSHOT_DIR, exist_ok=True)



# ---------------- MINIO ----------------
MINIO_ENDPOINT = 'http://minio:9000'#'http://10.161.67.41:9000'##'http://127.0.0.1:9000'# 'http://minio:9000'#'http://127.0.0.1:9000'
ACCESS_KEY = 'admin_minio'
SECRET_KEY = 'admin_minio'
BUCKET_NAME = 'ehskunshan'


# ---------------- sql server ----------------

META_COLUMNS = {'object_name':'object_name',
        'product_name':'product_name',
        'linespeed_spec':'linespeed_spec',
        'linespeed_real':'linespeed_real',
        'production_line':'production_line',
        'cameraid':'cameraid',
        'photo2check_bool':'photo2check_bool',
        'photo2minio_status':'photo2minio_status',
        'productnameid':'productnameid',
        'createtime_utc':'createtime_utc'
 }
