"""
YOLOv8 Detection Module
- 加载模型
- 图像预测
- 绘制检测结果
"""

from ultralytics import YOLO
import cv2
import torch
import pandas as pd
from config.config_meta import key_deteced_pre

#model_path = r'C:\Drivers\01_code_template\010_computer_vision\01_case_ppe\05_training_yolov8_v3_object_box\runs\detect\train4\weights\best.pt' 
model_path = r'/workspaces/my_project/project_files/01_models/best.pt' 
# 定义自定义颜色（BGR格式） 
custom_colors = { 0: (0, 0, 255), # 类别0：红色 
                 1: (0, 255, 0), # 类别1：绿色 
                 2: (0, 255, 255), # 类别2：黄色 
                 }

def load_model(model_path: str, device: str = None) -> YOLO:
    """加载 YOLO 模型"""
    device = device or ('0' if torch.cuda.is_available() else 'cpu')
    try:
        model = YOLO(model_path)
        print(f"模型加载成功: {model_path} | 设备: {device}")
        return model, device
    except FileNotFoundError:
        raise FileNotFoundError(f"模型文件不存在: {model_path}")

def predict(model: YOLO, source, device: str, conf: float = 0.75, iou: float = 0.45):
    """
    对单张或多张图片进行预测
    source: str / np.array / list
    返回 YOLO prediction 对象列表
    """
    results = model.predict(source=source, save=False, conf=conf, iou=iou, device=device)
    return results



def draw_boxes(img_bgr, xyxy_array, cls_array, conf_array, class_colors, name_mapping, thickness=4, font_scale=1.5):
    """
    在 BGR 图像上绘制所有检测框和标签
    xyxy_array: numpy array [N,4]，每行 [x1,y1,x2,y2]
    cls_array: numpy array [N]，类别索引
    conf_array: numpy array [N]，置信度
    class_colors: dict {cls_id: (B,G,R)}
    name_mapping: dict {cls_id: class_name}
    """
    for (x1, y1, x2, y2), cls_id, conf in zip(xyxy_array, cls_array, conf_array):
        x1, y1, x2, y2 = map(int, [x1, y1, x2, y2])
        color = class_colors.get(cls_id, (255, 255, 255))
        label = f"{name_mapping[cls_id]} {conf:.2f}"

        cv2.rectangle(img_bgr, (x1, y1), (x2, y2), color, thickness)
        (tw, th), baseline = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, font_scale, thickness)
        cv2.rectangle(img_bgr, (x1, y1 - th - baseline - 5), (x1 + tw, y1), color, -1)
        cv2.putText(img_bgr, label, (x1, y1 - baseline - 2), cv2.FONT_HERSHEY_SIMPLEX, font_scale, (0, 0, 0), thickness)
    return img_bgr

def check_alarm(arr_list):
    return 1 in arr_list #or (0 in arr_list and 1 in arr_list)#names: ['arm', 'tool_nok', 'tool_ok']

def pic_post_pre_df_annote(results, keys_minio, class_colors: dict, thickness=4, font_scale=1.5):
    annotated_dict = {}
    df_list = []
    name_mapping = results[0].names

    for r, object_name in zip(results, keys_minio):
        

        if len(r.boxes) == 0:
            df_list.append(pd.DataFrame([{
                'x1': None, 'y1': None, 'x2': None, 'y2': None,
                'cls': None, 'conf': None,
                'object_name': object_name,
                'detection_result': 'OK_EMPTY'
            }]))
        else:
            #img_bgr = cv2.cvtColor(r.orig_img, cv2.COLOR_RGB2BGR)
            img_bgr = r.orig_img
            cls_array = r.boxes.cls.cpu().numpy().astype(int)
            conf_array = r.boxes.conf.cpu().numpy()
            xyxy_array = r.boxes.xyxy.cpu().numpy()

            # 构建 DataFrame
            df_list.append(pd.DataFrame({
                'x1': xyxy_array[:,0], 'y1': xyxy_array[:,1],
                'x2': xyxy_array[:,2], 'y2': xyxy_array[:,3],
                'cls': cls_array, 'conf': conf_array,
                'object_name': object_name,
                'detection_result': ['NOK' if 1 in cls_array else 'OK_DETECTED'] * len(cls_array)# alarm logic
            }))

            # 绘制所有框
            img_bgr = draw_boxes(img_bgr, xyxy_array, cls_array, conf_array, class_colors, name_mapping, thickness, font_scale)
            #do not plot if no result detected
            annotated_dict[f'{key_deteced_pre}{object_name}'] = img_bgr

    df_result = pd.concat(df_list, ignore_index=True)
    df_result['cls_name'] = df_result['cls'].map(name_mapping)

    return df_result, annotated_dict