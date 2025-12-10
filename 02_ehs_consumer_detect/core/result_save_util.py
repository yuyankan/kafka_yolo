"""
Result Saving Module
- MinIO
- SQL
"""

import cv2
import pandas as pd
from core.minio_utl import store_yolo_image
from config.config_meta import BUCKET_NAME, tables_save
import core.myquery_db as query_db
import numpy as np

def save_to_minio(annotated_dict: dict, extension=".bmp", bucket_name=BUCKET_NAME):
    """保存标注图到 MinIO"""
    save_status = {}
    
    for object_name, img_bgr in annotated_dict.items():
        #img_bgr = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)
        success, buffer = cv2.imencode(extension, img_bgr)
        if not success:
            print(f"[ERROR] 转换 {object_name} 失败")
            continue
        frame_bytes = buffer.tobytes()
        content_type = 'image/' + extension.strip('.')
        save_status[object_name] = store_yolo_image(frame_data=frame_bytes,
                                                    object_name=object_name,
                                                    ContentType=content_type,
                                                    BUCKET_NAME=bucket_name)
    return save_status




def save_to_sql(df_data_raw_result,df_report_result):
    try:
        columns_raw = ['product_name','productnameid','linespeed_spec','linespeed_real','photo2check_bool','photo2minio_status','production_line','cameraid','topic_name','partition_id','createtime_utc','detection_result','object_name','partition_key']
         #get partion_key
        df_data_raw_result['createtime_utc'] = pd.to_datetime(df_data_raw_result['createtime_utc'], format='%Y%m%d%H%M%S')
        df_data_raw_result['quarid'] = (df_data_raw_result['createtime_utc'].dt.year * 10 + df_data_raw_result['createtime_utc'].dt.quarter).astype(int)
        df_data_raw_result['partition_key'] = df_data_raw_result['production_line']+ '_' +  df_data_raw_result['cameraid'].astype(int).astype(str) + '_' +df_data_raw_result['quarid'].astype(str)
        #object_name_na = (df_data_raw_result['object_name']=='') or (df_data_raw_result['object_name']=='na')
        #df_data_raw_result.loc[object_name_na,'product_name'] = df_data_raw_result.loc[object_name_na,'timestr']

        
        query_db.write_ksdata_updateorignore_duiplicate(df=df_data_raw_result[columns_raw],
                                                    unique_key_column=['partition_key','object_name'],
                                                    col_update=['detection_result'],
                                                    col_insert_rest=columns_raw[:-3],
                                                    col_auto_cal = ['partition_key'],
                                                    table_name=tables_save['result'],
                                                    unique_method='ignore'
                                                    )
        print(f"[INFO] 插入记录数量-df_data_raw_result: {len(df_data_raw_result)}")
        
        if df_report_result.empty:
            print('00--df_report_result is empty')
            return
        df_report_result = df_report_result.dropna(subset=['x1'])

        if df_report_result.empty:
            print('11--df_report_result is empty')
            return

       
        
        pk = f"{set(df_data_raw_result['partition_key'])}"
        obn = f"{set(df_data_raw_result['object_name'])}"
        query = f'''
        select id as rawresultdataid, object_name, partition_key
        from {tables_save['result']}
        where 1=1
        and partition_key in ({pk [1:-1]})
        and object_name in ({obn [1:-1]})

        '''
        print(query)
        df_data_raw_result_saved = query_db.query_ksdata(query)
        print(f'df_data_raw_result_saved: {df_data_raw_result_saved}')
    
        df_report_result_merged = df_report_result.merge(df_data_raw_result_saved, on=['object_name'], how='left')


        meta_sql_result_detail = ['x1','x2','y1','y2','conf','object_name_pre','photo2minio_status','cls_name','cls','detection_result','partition_key','rawresultdataid']
        df_report_result_merged = df_report_result_merged.fillna(np.nan)
        print(f"df_report_result_merged: {df_report_result_merged.head()}")

        query_db.write_ksdata_updateorignore_duiplicate(df=df_report_result_merged,
                                                        unique_key_column=['partition_key','rawresultdataid','cls'],
                                                        col_update=['cls_name'],
                                                        col_insert_rest=meta_sql_result_detail[:7],
                                                        table_name=tables_save['detail_result'],
                                                        unique_method='update')
        print(f"[INFO] 插入记录数量-df_report_result_merged: {len(df_report_result_merged)}")
    except Exception as e:
        print(e)
