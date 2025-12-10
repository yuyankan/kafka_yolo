
from ultralytics import YOLO
import cv2
import pandas as pd
import torch 
from core.minio_utl import store_yolo_image
import config.config_meta as cm

#model_path = r'C:\Drivers\01_code_template\010_computer_vision\01_case_ppe\05_training_yolov8_v3_object_box\runs\detect\train4\weights\best.pt'
model_path = r'./01_models/best.pt'

# 定义自定义颜色（BGR格式）
custom_colors = {
    0: (0, 0, 255),    # 类别0：红色
    1: (0, 255, 0),    # 类别1：绿色
    2: (0, 255, 255),  # 类别2：黄色
}



def plot_detections(result, class_colors, thickness=4, font_scale=1.5):
    """
    自定义绘制YOLOv8检测结果的函数
    
    参数:
        result: YOLOv8的检测结果对象
        class_colors: 字典，键为类别ID，值为BGR格式的颜色元组
        thickness: 边界框和文本的线宽
        font_scale: 文本字体大小
    
    返回:
        绘制了检测结果的图像
    """
    # 复制原始图像以避免修改原图
    image = result.orig_img.copy()
    
    # 遍历所有检测到的目标
    for box in result.boxes:
        # 解析边界框信息
        x1, y1, x2, y2 = map(int, box.xyxy[0])  # 左上角和右下角坐标
        conf = float(box.conf)  # 置信度
        cls_id = int(box.cls)   # 类别ID
        cls_name = result.names[cls_id]  # 类别名称
        
        # 获取该类别的颜色，如果没有定义则使用默认颜色(白色)
        color = class_colors.get(cls_id, (255, 255, 255))
        
        # 绘制边界框
        cv2.rectangle(image, (x1, y1), (x2, y2), color, thickness)
        
        # 准备标签文本
        label = f"{cls_name} {conf:.2f}"
        
        # 计算文本大小以确定背景框尺寸
        (text_width, text_height), baseline = cv2.getTextSize(
            label, cv2.FONT_HERSHEY_SIMPLEX, font_scale, thickness
        )
        
        # 绘制标签背景
        cv2.rectangle(
            image, 
            (x1, y1 - text_height - baseline - 5), 
            (x1 + text_width, y1), 
            color, 
            -1  # 填充矩形
        )
        
        # 绘制标签文本（白色文字）
        cv2.putText(
            image, 
            label, 
            (x1, y1 - baseline - 2), 
            cv2.FONT_HERSHEY_SIMPLEX, 
            font_scale, 
            #(255, 255, 255),  # 白色文字
            (0, 0, 0),  # 白色文字
            thickness
        )
    
    return image






def my_predict(model_path, pic_source):
    # --- 1. 确定设备 ---
    # 检查是否有可用的 CUDA 设备，如果有，则使用 '0' (第一张 GPU)，否则使用 'cpu'
    # 使用 '0' 比使用 'cuda' 更明确，因为它可以指定 GPU 编号
    device = '0' if torch.cuda.is_available() else 'cpu'
    print(f"正在使用的设备: {device}")

    # 1. 加载你训练好的模型
    try:
        model = YOLO(model_path)
        print(f"成功加载模型: {model_path}")
    except FileNotFoundError:
        print(f"错误：未找到模型文件 {model_path}。请检查路径是否正确。")
        return None # 失败时返回 None 或抛出异常

    # 2. 指定预测的图片源
    source = pic_source

    # 3. 进行预测并保存结果
    # 关键：通过 device 参数指定使用 GPU ('0') 或 CPU ('cpu')
    results = model.predict(
        source=source,
        save=False, 
        conf=0.5,
        iou=0.45,
        device=device # <--- 就是这里！指定设备为 '0' (GPU) 或 'cpu'
    )

    # 4. (可选) 遍历结果
    return results




def result_report_online(results,keys_minio):
    df_re = []
    name_mapping = results[0].names

    for r, p in zip(results, keys_minio):
        b = r.boxes
        df_temp = pd.DataFrame({
        'x1':b.xyxy[:,0].cpu().numpy(),
        'y1':b.xyxy[:,1].cpu().numpy(),
        'x2':b.xyxy[:,2].cpu().numpy(),
        'y2':b.xyxy[:,3].cpu().numpy(),
        'cls':b.cls.cpu().numpy().astype(int),
        'conf':b.conf.cpu().numpy(),
        })

        df_temp['pic_path_minio'] = p
        
        df_re.append(df_temp)
    df_result = pd.concat(df_re)
    df_result['cls_name'] = df_result['cls'].map(name_mapping)
    df_result['createtime_utc'] = pd.to_datetime(df_temp['pic_path_minio'].str.split('.'[0]).str.split('_')[-1], format='%Y%m%d%H%M%S')
    return df_result





from core.minio_utl import load_yolo_image
def save_result_minio(annotated_result_dict,extension=".bmp",BUCKET_NAME=cm.BUCKET_NAME):
    save_result_status = {}
    for object_name, image in annotated_result_dict.items():
        img_bgr = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)
        success, buffer = cv2.imencode(".bmp", img_bgr)
        if not success:
            print('not able to tranfer to byte!!!!')
            return
        print('*********process_frame_minio*********')

        #send to minio
        frame_bytes = buffer.tobytes()  # 转成二进制
        ContentType = 'image/' +extension.split('.')[-1]


        save_result_temp = store_yolo_image(frame_data=frame_bytes, 
                         object_name=object_name,
                         ContentType=ContentType,
                         BUCKET_NAME=BUCKET_NAME)
        save_result_status[object_name] = save_result_temp
    
    return save_result_status
        
    
    

import myquery_db as query_db

meta_sql_result = ['pic_path_minio','x1','x2','y1','y2','cls','conf','cls_name','createtime_utc','saveinmino_status']
def save_result_sql(df_result_report,save_result_status_mino):
    if df_result_report.empty:
        print('df_result_report is empty')
        return
    df_result_report['saveinmino_status'] = df_result_report['pic_path_minio'].map(save_result_status_mino)

    try:
        query_db.write_ksdata_updateorignore_duiplicate(df=df_result_report, 
                                                        unique_key_column=['pic_path_minio'],
                                                        col_update=meta_sql_result[1:], 
                                                        table_name='',
                                                        col_insert_rest=[], 
                                                        unique_method='update',
                                                       )
        print(f'insserted new detection:{ len(df_result_report)}')
    except Exception as e:
        print(f'ERROR: {e}')
    #query_db.write



def detect_annote(pic_dict,model_path=model_path,custom_colors=custom_colors,production_line='C11'):
    '''
    pic_source for yolov8 predict: 
        could be one pic: 'p1.jpg', or the one read: cv2.imread('image1.jpg')
        or multi one: ['p1.jpg', 'p1.jpg', 'p1.jpg'], or[cv2.imread('image1.jpg'), cv2.imread('image1.jpg') , cv2.imread('image1.jpg')]
        pic_dict:{key_minio:numpy_rgb}

    '''
    #predict
    pic_source= list(pic_dict.values())
    pic_kes = list(pic_dict.keys())
    prediction_result = my_predict(model_path=model_path,pic_source=pic_source)
    annotated_result = plot_detections(results=prediction_result, custom_colors=custom_colors)
    annotated_result_dict={}
    pic_kes_new = [f'detected{i}' for i in pic_kes]
    for k, v in zip(pic_kes_new, annotated_result):
        annotated_result_dict[k] = v
    
    #get record
    df_result_report = result_report_online(results=prediction_result,
                  keys_minio=pic_kes_new
                  )
    
    df_result_report[production_line]=production_line
    
    return df_result_report,annotated_result_dict



def work(pic_dict, model_path=model_path,custom_colors=custom_colors,production_line='C11'):
    df_result_report,annotated_result_dict = detect_annote(pic_dict=pic_dict,
                                                           production_line=production_line,
                                                           model_path=model_path,
                                                           custom_colors=custom_colors
                                                           )
    save_result_status = save_result_minio(annotated_result_dict=annotated_result_dict,
                                           extension=".bmp",
                                           BUCKET_NAME=cm.BUCKET_NAME)

    save_result_sql(df_result_report=df_result_report,
                    save_result_status_mino=save_result_status)





if __name__ == '__main__':
    model_path = model_path
    #model_path = r'C:\Drivers\01_code_template\010_computer_vision\01_case_ppe\05_training_yolov8_v3_object_box\runs\detect\train4\weights\best.pt'
    # 定义自定义颜色（BGR格式）
    custom_colors = {
        0: (0, 0, 255),    # 类别0：红色
        1: (0, 255, 0),    # 类别1：绿色
        2: (0, 255, 255),  # 类别2：黄色
    }


    df_result_report,annotated_result_dict = work(pic_dict=[],
                                                  model_path=model_path, 
                                                  custom_colors=custom_colors,
                                                  production_line='C11'
                                                  )
