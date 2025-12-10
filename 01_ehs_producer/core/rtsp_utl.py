import datetime

import os
import config.config_meta as cm



# --- 配置截图和日志 ---
SCREENSHOT_DIR = "screenshots_20250818"
LOG_FILE = "alarm_log2.txt"

if not os.path.exists(SCREENSHOT_DIR):
    os.makedirs(SCREENSHOT_DIR)

def log_alarm(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    print(log_entry.strip()) # 打印到控制台
    with open(LOG_FILE, "a") as f:
        f.write(log_entry)

# --- 配置NVR信息 ---
# --- 配置NVR信息 ---
def setup_nvr(nvr_ip_address= cm.nvr_ip_address,rtsp_port = cm.rtsp_port,username = cm.username ,password = cm.PASSWORD,channel_id= cm.channel_id):
    # --- 配置NVR信息 ---
    #nvr_ip_address = cm.nvr_ip_address # 替换为您的NVR的IP地址
    #rtsp_port = cm.rtsp_port                # RTSP端口，通常是554
    #username = cm.username                # 替换为您的NVR用户名
    #password = cm.password    # !!! 替换为您的NVR密码 !!!

    # 您想访问的摄像机在NVR上的通道号 (例如，NVR上的D1通道通常是1)
    # 假设您想访问的sensor (摄像机) 在NVR上被配置为通道1

    # 选择码流类型: 1为主码流, 2为子码流
    # 例如，通道1的主码流用 101, 子码流用 102
    # 通道2的主码流用 201, 子码流用 202, 以此类推
    stream_type_id_main = cm.stream_type_id_main # 主码流
    stream_type_id_sub = cm.stream_type_id_sub  # 子码流


    # 构建RTSP URL (此处以主码流为例)
    # rtsp_url = f"rtsp://{username}:{password}@{nvr_ip_address}:{rtsp_port}/Streaming/Channels/{channel_id}{stream_type_id_main}"
    # 或者更常见的拼接方式 (对于海康设备，如通道1主码流为101，通道13主码流为1301):
    rtsp_channel_identifier = str(channel_id) + stream_type_id_main
    rtsp_url = f"rtsp://{username}:{password}@{nvr_ip_address}:{rtsp_port}/Streaming/Channels/{rtsp_channel_identifier}"
    return rtsp_url,password



