# app.py
# 2025.12.08: video_stream_processor: CHANGE TO USE AV , to use only I-Frame; 
import cv2
import av
import time
import datetime
from config.kafka_config import producer,kafka_var
from core.rtsp_utl import setup_nvr
from core.kafka_util import send_to_kafka,BATCH_SIZE_KAFKA
from core.logger import log_alarm
import core.myquery_db as query_db
from core.myimage_pre import cut_image


from core.minio_utl import savepic_2minio_1pic,generate_save_path_minio
import config.config_meta as config
from config.kafka_config import kafka_var


vedio_freq = 3 # s ; seconde

# 
# ---------------- MinIO 保存 ----------------


# ---------------- Kafka 发送 ----------------


# ---------------- 帧处理 ----------------
def process_frame_minio(image,object_name,production_line='C11',save_dir_relative='rawpic',extension=".bmp",BUCKET_NAME=config.BUCKET_NAME):
    success, buffer = cv2.imencode(extension, image)
    if not success:
        print('not able to tranfer to byte!!!!')
        return
    print('*********process_frame_minio*********')

    #send to minio
    frame_bytes = buffer.tobytes()  # 转成二进制
    ContentType = 'image/' +extension.split('.')[-1]
    photo2minio_status = savepic_2minio_1pic(result=frame_bytes, 
                                       BUCKET_NAME=BUCKET_NAME, 
                                       object_name=object_name,
                                       ContentType=ContentType, 
                                       save_dir_relative=save_dir_relative,
                                       production_line=production_line, 
                                       extension=extension)
    
    return photo2minio_status


def process_frame_kafka(df_meta, production_line,cameraid):
    '''
    df_meta:product_name, production_line,linespeed_spec,linespeed_real,photo2check
    '''
    columns2send = kafka_var['ehs']['columns']
  
    print('*********process_frame_kafka*********')
       
    for i in columns2send:
        if i not in df_meta:
            print(f'{i} not in df_meta:',df_meta.head())
            df_meta[i] = ''
    

    send_to_kafka(df=df_meta, 
                  topic=kafka_var['ehs']['topic'], 
                  columns= kafka_var['ehs']['columns'], 
                  production_line=production_line, 
                  cameraid=cameraid,
                  batch_size=BATCH_SIZE_KAFKA)


def process_and_send(df_meta,image,object_name,production_line='C11',cameraid=4,save_dir_relative='rawpic',extension=".bmp",BUCKET_NAME=config.BUCKET_NAME):
    photo2minio_status = process_frame_minio(image=image,object_name=object_name,
                                                         production_line=production_line,
                                                         save_dir_relative=save_dir_relative,
                                                         extension=extension,
                                                         BUCKET_NAME=BUCKET_NAME)
 
    df_meta[config.META_COLUMNS['photo2minio_status']] = photo2minio_status

    process_frame_kafka(df_meta=df_meta,
                         production_line=production_line,
                         cameraid=cameraid
                         )
    
  

def skip_logic(df_meta):   

    df_meta['linespeed_real'] = df_meta['linespeed_real'].astype(float)   
    df_meta['linespeed_spec'] = df_meta['linespeed_spec'].astype(float)
    #check logic
    if df_meta['linespeed_real'].values[0] <=(df_meta['linespeed_spec'].values[0]/2):
        return True
    return False

def read_current_meta(production_line='C11',cameraid=4):
    '''
    get: current product name, line speed_spec, linespeed_real
    '''
    query = f'''
with pn as 
    (
        -- 子查询 T1: 获取最新的 'Product Number'
        SELECT TOP 1 
            kcn.[_VALUE]  COLLATE Chinese_PRC_CI_AS AS product_name
        FROM 
            KEP_KSP_DATA.dbo.KSP_C11_2_New AS kcn
        WHERE 
            kcn.[_NAME] = 'KSP_C11.Device1.C11_OPCDA.C11.Product Number'
            
        ORDER BY 
            kcn.[_TIMESTAMP] DESC
    ),
    ls as 
    (
        -- 子查询 T2: 获取最新的 'Line Speed'
        SELECT TOP 1 
            kcn.[_VALUE] AS linespeed_real
        FROM 
            KEP_KSP_DATA.dbo.KSP_C11_1_New AS kcn
        WHERE 
            kcn.[_NAME] = 'KSP_C11.Device1.C11_OPCDA.C11.Line Speed'
        ORDER BY 
            kcn.[_TIMESTAMP] DESC
    ),
    temp_re as (
    SELECT pn.product_name, mpg.production_speed as linespeed_spec,mp.id productnameid
    from pn
    join ks_project_yyk.ods.meta_product as mp on mp.product_name =pn.product_name and mp.production_line='{production_line}'
    join ks_project_yyk.ods.meta_product_group mpg on mpg.id =mp.productgroupid
    )
    select temp_re.*, ls.linespeed_real
    from temp_re
    cross join ls
    ;
    '''
    print(query)
    flag_skippic = True # skip pic if true
    flag_empy = False # skip meta if true
    df_meta = query_db.query_ksdata(query=query)

    if df_meta.empty:
        flag_skippic = True
        flag_empy = True
    
    else:    
        # 2. 格式化日期和时间字符串（基于 UTC 时间）
       
     
        flag_skippic = skip_logic(df_meta=df_meta)
        df_meta['production_line'] = production_line
        df_meta['cameraid'] = cameraid
        
        df_meta['photo2minio_status'] = 0
        df_meta['photo2check_bool'] = 0 if flag_skippic else 1
   

    
    return flag_skippic,df_meta,flag_empy



  

def video_stream_processor(rtsp_url: str, password: str, 
                           production_line: str = 'C11',
                           cameraid: int = config.channel_id,
                           vedio_freq: float = 5.0,
                           save_dir_relative: str='rawpic',
                           extension: str=".BMP"
                           ):
    """
    视频流处理器（PyAV I-Frame解码，条件先行，自动重连）
    """
    print(f"尝试连接到RTSP流: {rtsp_url.replace(password, '********')}")

    # -----------------------------
    # 重连函数
    # -----------------------------
    def open_stream(url):
        try:
            container = av.open(url)
            print("视频流连接成功！")
            return container
        except av.AVError as e:
            log_alarm(f"错误：无法打开视频流 {url} -> {e}")
            return None

    container = open_stream(rtsp_url)
    if container is None:
        return

    # -----------------------------
    # 丢弃前几帧，确保缓冲
    # -----------------------------
    discard_count = 0
    for frame in container.decode(video=0):
        discard_count += 1
        if discard_count > 10:
            break
        print(f"丢弃帧 {discard_count}")

    last_check_time = time.time()

    # -----------------------------
    # 主循环
    # -----------------------------
    while True:
        current_time = time.time()

        # -----------------------------
        # 1️⃣ 先判断条件
        # -----------------------------
        flag_skippic, df_meta, flag_empy = read_current_meta(
            production_line=production_line,
            cameraid=cameraid
        )

        # 条件不满足或频率未到，短暂休眠
        print(f'flag_skippic:{flag_skippic},flag_empy-{flag_empy}')
        print(f'current_time - last_check_time:{current_time - last_check_time}')
        print(df_meta.head())
        if flag_empy:
            print(df_meta.head())
            time.sleep(20) # no data
            continue
     
     
        print(f'22222-current_time - last_check_time:{current_time - last_check_time},last_check_time{last_check_time}, current_time:{current_time}')

        if (current_time - last_check_time) < vedio_freq:
           
            time.sleep(5)
            continue
        print('2222222222222222222')

        # -----------------------------
        object_name,time_str = generate_save_path_minio(production_line=production_line,
                                                       save_dir_relative=save_dir_relative,
                                                       extension=extension
        )
        # 2️⃣ 只发送元数据，不拉帧
        df_meta['object_name'] = object_name
        df_meta['createtime_utc'] = time_str
       
        # -----------------------------
        if flag_skippic:
            print('process_frame_kafka_only')
            process_frame_kafka(
                df_meta=df_meta,
                production_line=production_line,
                cameraid=cameraid
            )
            last_check_time = current_time
            time.sleep(60) # speed is 0 or half speed , do not need check frequencyly!!!
            continue  # 下一轮循环

        # -----------------------------
        # 3️⃣ 条件满足，需要处理图片 → 拉取关键帧
        # -----------------------------
        frame_obj = None
        try:
            for f in container.decode(video=0):
                if f.key_frame:
                    frame_obj = f
                    break

            if frame_obj is None:
                log_alarm("无法获取关键帧，尝试重连...")
                container.close()
                time.sleep(5)
                container = open_stream(rtsp_url)
                if container is None:
                    log_alarm("重连失败，程序退出。")
                    break
                continue

            # 转为 BGR numpy array，用于 OpenCV / 推理
            frame = frame_obj.to_ndarray(format='bgr24')
            #cut image
            frame_cut = cut_image(image=frame,  
                      center_w_ratio=0, 
                      center_h_ratio=0.08, 
                      width_ratio=0.2, 
                      height_ratio=0.3)

        except av.AVError as e:
            log_alarm(f"视频流解码异常: {e}, 尝试重连...")
            container.close()
            time.sleep(5)
            container = open_stream(rtsp_url)
            if container is None:
                log_alarm("重连失败，程序退出。")
                break
            continue

        # -----------------------------
        # 4️⃣ 处理图片
        # -----------------------------
        print('process_and_send')
        process_and_send(
            df_meta=df_meta,
            image=frame_cut.copy(),
            object_name=object_name,
            production_line=production_line,
            cameraid=cameraid,
            save_dir_relative='rawpic',
            extension=".bmp",
            BUCKET_NAME=config.BUCKET_NAME
        )

        last_check_time = current_time

    # -----------------------------
    # 清理
    # -----------------------------
    container.close()
    cv2.destroyAllWindows()
    producer.flush()
    log_alarm("程序已停止。")




def run_cycle(production_line,cameraid):
    """主入口函数，持续运行视频流处理器。"""
    # 只需要连接一次视频流
    rtsp_url, PASSWORD = setup_nvr(channel_id=cameraid)
    video_stream_processor(rtsp_url=rtsp_url, 
                           password=PASSWORD,
                           production_line=production_line,
                           cameraid=cameraid)


