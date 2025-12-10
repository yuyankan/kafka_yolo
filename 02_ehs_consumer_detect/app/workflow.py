"""
Workflow Module
- 整合检测、绘图、保存到 MinIO 与 SQL
"""

from core.detection_util import load_model, predict, pic_post_pre_df_annote
from core.result_save_util import save_to_minio, save_to_sql
from core.minio_utl import load_yolo_image
from config.config_meta import BUCKET_NAME, custom_colors,key_deteced_pre
import cv2
import pandas as pd



# detect and annotate pic
def detect_annotate(pic_dict: dict, model, device, class_colors):
    keys_o = list(pic_dict.keys())
    images = list(pic_dict.values())

    #model, device = load_model(model_path)
    results = predict(model, images, device=device)

    df_report, annotated_dict = pic_post_pre_df_annote(results=results, 
                                 keys_minio=keys_o, 
                                 class_colors=class_colors, 
                                 thickness=4, 
                                 font_scale=1.5)
    return df_report, annotated_dict



def load_picturesfrom_minio(df_data_checkpic):
    pic_dic = {}
    for obj in df_data_checkpic['object_name'].unique():
        # try to async
        img_temp_bgr = load_yolo_image(object_name=obj,bucket=BUCKET_NAME)
        if  img_temp_bgr is None:
            continue
        #img_rgb = cv2.cvtColor(img_temp, cv2.COLOR_BGR2RGB) # for yolo-bgt
        pic_dic[obj] = img_temp_bgr
    return pic_dic




def process_data_custom(df_data, model, device):
    if df_data.empty:
        return
    #deal with pic:load
    # need to check and pic uploaded
    df_data['photo2check_bool']=df_data['photo2check_bool'].astype(int)
    conda2check_pic = ((df_data['photo2check_bool']==1)&(df_data['photo2minio_status'])==True)
    df_data_checkpic = df_data[conda2check_pic]
    print('df_data_checkpic',df_data_checkpic)
    pic_dic = None
    if not df_data_checkpic.empty:
        pic_dic = load_picturesfrom_minio(df_data_checkpic)
        #print('pic_dic:',pic_dic)
    
    if not pic_dic:
        df_data_raw_result = df_data.copy()
        df_data_raw_result['detection_result'] = 'na'
        # send /save df_data_raw_result only
        save_to_sql(df_data_raw_result=df_data_raw_result,df_report_result=pd.DataFrame())

        return
     #save result

  
    
    #deal with pic: detect
        # detect and annotate pic
    df_report, annotated_dict = detect_annotate(pic_dict=pic_dic, 
                                                model=model, 
                                                device=device,
                                                class_colors=custom_colors, 

                                                )
    print('df_report:',df_report.head())
    df_report_status = df_report[['object_name','detection_result']].drop_duplicates()

    #result combine to raw
    df_data_raw_result = df_data.merge(df_report_status, on=['object_name'], how='left')
    #df_report_result = df_report.merge(df_data[['object_name','productnameid','production_line','cameraid']],on=['object_name'],how='left')
    df_report_result = df_report.copy()
    df_report_result['object_name_pre'] = key_deteced_pre

    #save result
    if not annotated_dict:
        print('save_status_dict is EMPTY')
        save_status_dict = {}

    else: 
        save_status_dict = save_to_minio(annotated_dict)
    df_report_result['photo2minio_status'] ='na'
    df_report_result['photo2minio_status'] = (df_report_result['object_name_pre']+df_report_result['object_name']).map(save_status_dict)

    #if 'createtime_utc' not in df_data_raw_result.columns:

    #    df_data_raw_result['timestr'] = df_data_raw_result['object_name'].str.split('_').str[-1].str[:-4]
                                         
    #    df_data_raw_result['createtime_utc'] = pd.to_datetime(
    #                                        df_data_raw_result['time_str'],
    #                                        format='%Y%m%d%H%M%S'
    #                                    )

    print('df_report_result:',df_report_result.head())
    print('df_data_raw_result:',df_data_raw_result.head())

    save_to_sql(df_data_raw_result=df_data_raw_result,df_report_result=df_report_result)
    #return df_report, annotated_dict, save_status
    #save_to_sql(df_data_raw_result)
 
    
