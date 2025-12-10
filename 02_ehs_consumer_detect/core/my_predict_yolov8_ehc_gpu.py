
from ultralytics import YOLO
import cv2
import pandas as pd
import os
import datetime
import torch 


#model_path = r'C:\Drivers\01_code_template\010_computer_vision\01_case_ppe\05_training_yolov8_v3_object_box\runs\detect\train4\weights\best.pt'
model_path = r'./01_models/best.pt'

# 定义自定义颜色（BGR格式）
custom_colors = {
    0: (0, 0, 255),    # 类别0：红色
    1: (0, 255, 0),    # 类别1：绿色
    2: (0, 255, 255),  # 类别2：黄色
}

# 创建保存目录
original_filename = '/workspaces/my_project/project_files/100_images'
save_dir_relative = 'test3_save'

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



def save2local(image, save_path):
    '''
    do not transfer from like rgb 2 gbr
    '''
    cv2.imwrite(save_path, image)
        #print(f"已保存结果: {save_path}")
        


def my_predict_cpu(model_path,pic_source):

    # 1. 加载你训练好的模型
    # 假设你的模型保存在 runs/detect/train/weights/best.pt
    #model_path = 'runs/detect/train4/weights/best.pt' 
    # 请替换为你的模型路径，例如：'runs/detect/train/weights/best.pt'

    try:
        model = YOLO(model_path)
        print(f"成功加载模型: {model_path}")
    except FileNotFoundError:
        print(f"错误：未找到模型文件 {model_path}。请检查路径是否正确。")
        exit()

    # 2. 指定预测的图片源
    # 可以是单张图片、图片文件夹、视频文件或视频流
    # 例如：
    # source = 'path/to/your/image.jpg'
    # source = 'path/to/your/images_folder/'
    source = pic_source # 请替换为你的图片文件夹路径

    # 3. 进行预测并保存结果
    # YOLOv8 的 predict 方法非常方便，可以自动处理画框和保存
    results = model.predict(
        source=source,
        save=False,          # 自动保存结果图片
        conf=0.5,           # 设置置信度阈值，低于此值的检测结果将被忽略
        iou=0.45            # 设置 IoU 阈值，用于非极大值抑制（NMS）
    )

    # 4. (可选) 遍历结果
    # 你可以通过遍历 results 对象来获取每个图片的详细预测信息
    return results


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





def _generate_save_path(save_dir_relative, production_line):
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
    global original_filename
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    
    # 2. 格式化日期和时间字符串（基于 UTC 时间）
    date_path = now_utc.strftime('%Y%m%d') # UTC 的 年/月/日
    time_str = now_utc.strftime('%Y%m%d%H%M%S') # UTC 的 年月日时分秒
    # 3. 提取原始文件名（不含扩展名）
    # 使用 os.path.basename() 更具跨平台性
    #original_filename = os.path.splitext(os.path.basename(original_path))[0]
    
   
    # 5. 构建完整保存路径
    # save_dir/年/月/日/产线_imae_年月日时分秒.BMP
    save_directory = os.path.join(original_filename,save_dir_relative, production_line,date_path)

    # 6. 确保目录存在
    try:
    # result.path 假设是图片的原始路径，用于提取信息
        os.makedirs(save_directory, exist_ok=True)
    
    except Exception as e:
        print(f"⚠️ 无法生成保存路径: {e}")
        return None, None

    
    return save_directory,time_str

def savepic_2local_optimized(results, save_dir_relative, custom_colors, production_line="C11", extension=".BMP"):
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
    save_dir_list = []
    save_directory,time_str = _generate_save_path(save_dir_relative=save_dir_relative,
                                                    production_line=production_line, 

                                                  )
    if not save_directory:
        print(f"⚠️ 无法生成保存路径: ")
        return
    
    
    # 使用 enumerate 获取索引，但这里仅用于计数，如果不需要可以简化为 for result in results:
    for i, result in enumerate(results): 

        # 2. 绘制边界框和标签
        # 假设 plot_detections 返回一个 PIL.Image 或 numpy.ndarray 对象
        annotated_img = plot_detections(result, custom_colors)
        
        # 3. 保存图像
        try:
            # save2local 需要能处理 annotated_img 并保存到 save_path
            save_path = os.path.join(save_directory, f'image_{time_str}_{i}{extension}')
            save2local(annotated_img, save_path)
            
            # 记录成功保存的路径和文件名
            save_dir_list.append(save_path)
      
            
        except Exception as e:
            print(f"❌ 保存图片 {save_path} 失败: {e}")
            # 可以根据需要决定是继续还是中断

    print(f"\n✅ 预测完成。{len(save_dir_list)} 张结果图片已保存到 {save_directory} 的日期子文件夹中。")
    return  save_dir_list,time_str


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

        df_temp['pic_path'] = p
        
        df_re.append(df_temp)
    df_result = pd.concat(df_re)
    df_result['cls_name'] = df_result['cls'].map(name_mapping)
    df_result['createtime_utc'] = pd.to_datetime(df_temp['pic_path'].str.split('.'[0]).str.split('_')[-1], format='%Y%m%d%H%M%S')
    return df_result




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

   
    
    #get record
    df_result_report = result_report_online(results=prediction_result,
                  keys_minio=pic_kes
                  )
    
    df_result_report[production_line]=production_line
    
    return df_result_report

def work(pic_source,model_path=model_path,save_dir_relative=save_dir_relative,custom_colors=custom_colors,production_line='C11'):
    '''
    pic_source for yolov8 predict: 
        could be one pic: 'p1.jpg', or the one read: cv2.imread('image1.jpg')
        or multi one: ['p1.jpg', 'p1.jpg', 'p1.jpg'], or[cv2.imread('image1.jpg'), cv2.imread('image1.jpg') , cv2.imread('image1.jpg')]

    '''
    #predict
    prediction_result = my_predict(model_path=model_path,pic_source=pic_source)

    #save result
    save_dir_list,time_str = savepic_2local_optimized(results=prediction_result,
                                                         save_dir_relative=save_dir_relative,
                                                         custom_colors=custom_colors)
    
    #get record
    df_result_report = result_report(results=prediction_result,
                  save_dir_list=save_dir_list,
                  time_str=time_str
                  )
    
    df_result_report[production_line]=production_line
    
    return df_result_report


if __name__ == '__main__':
    model_path = model_path
    #model_path = r'C:\Drivers\01_code_template\010_computer_vision\01_case_ppe\05_training_yolov8_v3_object_box\runs\detect\train4\weights\best.pt'
    # 定义自定义颜色（BGR格式）
    custom_colors = {
        0: (0, 0, 255),    # 类别0：红色
        1: (0, 255, 0),    # 类别1：绿色
        2: (0, 255, 255),  # 类别2：黄色
    }
    folder_2predict = r'C:\Drivers\01_code_template\010_computer_vision\01_case_ppe\03_training_yolov8\pic2predict\test3'
    save_dir = 'runs/detect/custom_predict'


    df_result_report = work(model_path=model_path, 
                            pic_source=folder_2predict,
                            save_dir=save_dir,
                            custom_colors=custom_colors)
