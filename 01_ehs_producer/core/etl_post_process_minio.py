import cv2
import pandas as pd
import os
import datetime
import etl_minio as em




#model_path = r'C:\Drivers\01_code_template\010_computer_vision\01_case_ppe\05_training_yolov8_v3_object_box\runs\detect\train4\weights\best.pt'
model_path = r'./01_models/best.pt'

# 定义自定义颜色（BGR格式）
custom_colors = {
    0: (0, 0, 255),    # 类别0：红色
    1: (0, 255, 0),    # 类别1：绿色
    2: (0, 255, 255),  # 类别2：黄色
}

# 创建保存目录


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

# 示例调用
# 假设你的模型路径和图片源
# model_path = 'runs/detect/train4/weights/best.pt'
# pic_source = 'path/to/your/image.jpg'
# prediction_results = my_predict(model_path, pic_source)



def _generate_save_path_minio(production_line,save_dir_relative='rawpic',extension=".BMP"):
    """
    根据给定的规则生成完整的、带日期层级的保存路径。
    
    Args:
        save_dir (str): 基础保存目录。
        original_path (str): 结果对象中的原始路径，用于提取文件名。
        line_id (str): 用于文件名前缀的标识符（例如 '产线_imae'）。
        extension (str): 目标文件扩展名（例如 '.BMP'）。
        
    Returns:
        str: 完整的保存路径。
        str: 提取出的不带扩展名的文件名部分。
    """
    # 1. 获取当前时间

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    
    # 2. 格式化日期和时间字符串（基于 UTC 时间）
    date_path = now_utc.strftime('%Y%m%d') # UTC 的 年/月/日
    time_str = now_utc.strftime('%Y%m%d%H%M%S') # UTC 的 年月日时分秒
    # 3. 提取原始文件名（不含扩展名）
    # 使用 os.path.basename() 更具跨平台性
    #original_filename = os.path.splitext(os.path.basename(original_path))[0]
    
   
    # 5. 构建完整保存路径
    # save_dir/年/月/日/产线_imae_年月日时分秒.BMP
   
    object_name = f"{save_dir_relative}/{production_line}/{date_path}/image_{time_str}{extension}"

    
    return object_name


def savepic_2minio_1pic(result,  ContentType='rawpic', save_dir_relative='rawpic',production_line="C11", extension=".BMP"):
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

    save_result = False
    try:
        # save2local 需要能处理 annotated_img 并保存到 save_path
        save_result = em.store_yolo_image(frame_data=result, object_name=object_name,ContentType=ContentType)
        print(f" 保存图片 {object_name} OK: {e}")

        
    except Exception as e:
        print(f"❌ 保存图片 {object_name} 失败: {e}")
        # 可以根据需要决定是继续还是中断

    return  object_name,save_result






def result_report(results,save_dir_list,time_str):
    df_re = []
    name_mapping = results[0].names

    for r, p in zip(results, save_dir_list):
        b = r.boxes
        df_temp = pd.DataFrame({
        'x1':b.xyxy[:,0].cpu().numpy(),
        'y1':b.xyxy[:,1].cpu().numpy(),
        'x2':b.xyxy[:,2].cpu().numpy(),
        'y2':b.xyxy[:,3].cpu().numpy(),
        'cls':b.cls.cpu().numpy().astype(int),
        'conf':b.conf.cpu().numpy(),
        })

        df_temp['pic_path'] = p
        
        df_re.append(df_temp)
    df_result = pd.concat(df_re)
    df_result['cls_name'] = df_result['cls'].map(name_mapping)
    df_result['createtime_utc'] = time_str
    return df_result


def work_post(prediction_result,production_line,save_dir_relative='rawpic'):
     #save result
    save_dir_list,time_str = savepic_2minio_1pic(results=prediction_result,
                                                         save_dir_relative=save_dir_relative,
                                                         custom_colors=custom_colors)
    
    #get record
    df_result_report = result_report(results=prediction_result,
                  save_dir_list=save_dir_list,
                  time_str=time_str
                  )
    
    df_result_report[production_line]=production_line
    
    return df_result_report



