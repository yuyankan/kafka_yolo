'''
plt.imshow()
作用： 这是显示图像的核心函数。它接收一个图像数组（例如 NumPy 数组）作为输入，并在 Matplotlib 的坐标系中将它渲染成一个图像。

参数：

X：要显示的图像数据。对于彩色图像，通常是一个形状为 (M, N, 3) 或 (M, N, 4) 的数组，代表 RGB 或 RGBA 通道。

cmap：用于单通道灰度图像的颜色映射（color map）。例如，cmap='gray' 会将灰度值映射到灰色调。

interpolation：插值方法，用于图像缩放。常见的选项有 'nearest'（最近邻，适合像素画）和 'bilinear'（双线性插值，适合平滑图像）。

例子： plt.imshow(rgb_image) 会在 Matplotlib 的画布上放置你的图像，但它并不会立即弹出窗口。


%matplotlib inline 是一个魔法命令 (Magic Command)，它只在 IPython 环境中有效，例如在 Jupyter Notebook 和 IPython shell 中
'''

import cv2
import matplotlib.pyplot as plt
#%matplotlib inline
import math
import numpy as np

def show_pic_simple(images_list, titles_list=None, max_cols=3):
    """
    简洁版函数：在一个网格中显示多张图片。
    
    参数:
    images_list (list): 包含图片对象的列表。
    titles_list (list, optional): 图片标题的列表。
    max_cols (int): 每行最多显示的图片数量。
    """
    image_num = len(images_list)
    if image_num == 0:
        print("image list is empty")
        return
    
    if not isinstance(images_list, list):
        print(f'images_list type is {type(images_list)}, have to list')
        return



    # 计算行数和列数
    
    cols = min(image_num, max_cols)
    rows = math.ceil(image_num / cols)


    # 创建子图
    fig, axes = plt.subplots(rows, cols, figsize=(cols * 5, rows * 5), squeeze=False)

    # 将二维axes数组展平为一维数组，方便索引
    flat_axes = axes.flatten()

    for i, ax in enumerate(flat_axes):
        if i < image_num:
            if images_list[i].ndim >2:
                ax.imshow(images_list[i])
            else: ax.imshow(images_list[i], cmap='gray')
            if titles_list and i < len(titles_list):
                ax.set_title(titles_list[i])
            else:
                ax.set_title(f'pic {i+1}')
            ax.axis('off')
        else:
            # 隐藏多余的子图
            ax.axis('off')

    plt.tight_layout()
    plt.show()



# Read the image
def read_image_cv2(image_path,cvt2rgb=False):
    image_type ='BGR'
    image = cv2.imread(image_path) # BGR
    if cvt2rgb:
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        image_type = 'RGB'
    
    print(f'IMAGE IS {image_type}, shape HxWxd3: {image.shape}')
    return image





def cut_image(image,  center_w_ratio, center_h_ratio, width_ratio, height_ratio):
    #center ration: -0.5-0.5
    # height, width ratio: 0-1
    # 定义感兴趣区域 (ROI) 的坐标
    # 格式为 (x_min, y_min, width, height)
    print('NOTE: image shape should be: H x W x (3))!!!')
    h, w, _= image.shape
    center_h = int((center_h_ratio +0.5) * h)
    center_w = int((center_w_ratio + 0.5) * w)
    height_half = int(height_ratio * h/2)
    width_half = int(width_ratio * w/2)
    
    # 从原始图片中裁剪出 ROI 区域
    roi = image[center_h-height_half:center_h+height_half, center_w-width_half:center_w+width_half] # slicie: reverse dimension direction!!!: h x W X 3 ([r, g, b] value)
    return roi



def adjust_hsv_properties_rgb(image_rgb, saturation_factor, brightness_factor):
    """
    通过转换为 HSV 色彩空间来调整图片的饱和度和亮度。
    
    参数:
    image_path (str): 图片文件的路径。
    saturation_factor (float): 饱和度的增强因子。
    brightness_factor (float): 亮度的增强因子。

    返回:
    PIL.Image.Image: 处理后的图片对象。
    """

    # 1. RGB -> HSV
    hsv_array = cv2.cvtColor(image_rgb, cv2.COLOR_RGB2HSV) # cv2.COLOR_RGB2HSV_FULL

    # 2. 直接修改 S (饱和度) 和 V (亮度) 通道
    # S 通道位于索引 1，V 通道位于索引 2
    #hsv_array = hsv_array.astype(np.float32)
    #saturation_factor_int = int(1/saturation_factor) if saturation_factor!=0 else 1
    #brightness_factor_int = int(1/brightness_factor) if brightness_factor!=0 else 1
    temp_s = (hsv_array[:, :, 1]*saturation_factor)# s: 离灰度-纯色的距离; note :while change to black ,as uint8 up limit is 255, will chagne to 0
    temp_b = hsv_array[:, :, 2] * brightness_factor#v: bright: 黑-白的距离
    # clip to 0-255 first
    temp_s = np.clip(temp_s, 0, 255).astype(np.uint8) # clip first !!!! as uint8 up limit is 255, will change like white 300 to 0
    temp_b = np.clip(temp_b, 0, 255).astype(np.uint8)

    #hsv_array[:, :, 1] = (hsv_array[:, :, 1]*saturation_factor).astype(np.uint8) 
    hsv_array[:, :, 1] = temp_s
    hsv_array[:, :, 2] = temp_b

    #hsv_array[:, :, 1] = hsv_array[:, :, 1] // saturation_factor_int
    #hsv_array[:, :, 2] = hsv_array[:, :, 2] // brightness_factor_int
    
    # 限制值在 0-255 范围内，防止溢出
    #hsv_array = np.clip(hsv_array, 0, 255)
    #hsv_array = hsv_array.astype(np.uint8)

    # 3. HSV -> RGB
    rgb_image_adjusted = cv2.cvtColor(hsv_array, cv2.COLOR_HSV2RGB)
    
    return rgb_image_adjusted




def image2grey(image,cur_type):
    cvt_fun = f'cv2.COLOR_{cur_type.upper()}2GRAY'
    gray_image = cv2.cvtColor(image, eval(cvt_fun))
    return gray_image

def image_contrast_scale(image,parameters={'alpha':1.2,'beta':0}):


    image_scale = cv2.convertScaleAbs(image, alpha=parameters['alpha'], beta=parameters['beta'])
    return image_scale

def image_contrast_clahe(image, parameters={'clipLimit':2.0,'tileGridSize':(8, 8)}):
    # 3. 增强灰度图的对比度
    # 我们将使用 CLAHE (自适应直方图均衡化) 来增强对比度
    # 这种方法在增强对比度的同时能更好地保留局部细节


    # 创建 CLAHE 对象
    # clipLimit 限制对比度增强的程度
    # tileGridSize 定义了用于均衡化的小块区域大小
    clahe = cv2.createCLAHE(clipLimit=parameters['clipLimit'], tileGridSize=parameters['tileGridSize'])
    # 应用 CLAHE 到灰度图
    image_clahe = clahe.apply(image)
    return image_clahe


def image_contrast_hist(image):
    image_hist = cv2.equalizeHist(image)
    return image_hist



def modify_contrast(image, **method_used):
    image_result_dict = {}
    for k in method_used:
        if 'scale' in k:
            image_result_dict[k] = image_contrast_scale(image, method_used[k])
        elif 'hist' in k:
            image_result_dict[k] = image_contrast_hist(image)
        elif 'clahe':
            image_result_dict[k] = image_contrast_clahe(image, method_used[k])

    
    return image_result_dict



def save_image(image2save_list:list, image_channel:str='BGR', image_path:str ='./',image_name_list:list =[], image_type:str =''):
    print('make sure image is BGR!!!')
    if image_channel.upper() !='BGR':
        fun_tran = f'cv2.COLOR_{image_channel.upper()}2BGR'
        image2save_list = [cv2.cvtColor(i, eval(fun_tran)) for i in image2save_list]


    if len(image_name_list)<1:
        image_name_list = [f'pic_{i}' for i in range(len(image2save_list))]
    if image_path[-1] !='/':
        image_path += '/'
    
    for n, im in zip(image_name_list, image2save_list):
        save_p = f'{image_path}{n}.{image_type}'
        print(save_p)
        cv2.imwrite(save_p, im)