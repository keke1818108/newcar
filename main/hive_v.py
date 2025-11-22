import configparser
import re
import json
import os
import mysql.connector
from django.http import JsonResponse
from hdfs import InsecureClient
from pyhive import hive
import csv
from util.configread import config_read
from util.CustomJSONEncoder import CustomJsonEncoder
from util.codes import normal_code, system_error_code
# 获取当前文件路径的根目录
parent_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
m_username = "Administrator"
hadoop_client = InsecureClient('http://localhost:9870')
dbtype, host, port, user, passwd, dbName, charset,hasHadoop = config_read(os.path.join(parent_directory,"config.ini"))

#将mysql里的相关表转成hive库里的表
def migrate_to_hive():

    mysql_conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=passwd,
        database=dbName
    )
    cursor = mysql_conn.cursor()

    hive_conn = hive.Connection(
        host='localhost',
        port=10000,
        username=m_username,
    )
    hive_cursor = hive_conn.cursor()
    #创建Hive数据库（如果不存在）
    hive_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {dbName}")
    hive_cursor.execute(f"USE {dbName}")

    qichexinxi_table_path=f'/user/hive/warehouse/{dbName}.db/qichexinxi'
    #删除已有的hive表
    if hadoop_client.status(qichexinxi_table_path,strict=False):
        hadoop_client.delete(qichexinxi_table_path, recursive=True)
    # 在Hive中删除表
    qichexinxi_drop_table_query = f"""DROP TABLE qichexinxi"""
    hive_cursor.execute(qichexinxi_drop_table_query)
    cursor.execute("SELECT * FROM qichexinxi")
    qichexinxi_column_info = cursor.fetchall()
    #将数据写入 CSV 文件
    with open(os.path.join(parent_directory, "qichexinxi.csv"), 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # 写入数据行
        for row in qichexinxi_column_info:
            writer.writerow(row)
    cursor.execute("DESCRIBE qichexinxi")
    qichexinxi_column_info = cursor.fetchall()
    create_table_query = "CREATE TABLE IF NOT EXISTS qichexinxi ("
    for column, data_type, _, _, _, _ in qichexinxi_column_info:
        match = re.match(r'(\w+)(\(\d+\))?', data_type)
        mysql_type = match.group(1)
        hive_data_type = get_hive_type(mysql_type)
        create_table_query += f"{column} {hive_data_type}, "
    qichexinxi_create_table_query = create_table_query[:-2] + ") row format delimited fields terminated by ','"
    hive_cursor.execute(qichexinxi_create_table_query)
    # 上传映射文件
    qichexinxi_hdfs_csv_path = f'/user/hive/warehouse/{dbName}.db/qichexinxi'
    hadoop_client.upload(qichexinxi_hdfs_csv_path, os.path.join(parent_directory, "qichexinxi.csv"))
    qichepeizhi_table_path=f'/user/hive/warehouse/{dbName}.db/qichepeizhi'
    #删除已有的hive表
    if hadoop_client.status(qichepeizhi_table_path,strict=False):
        hadoop_client.delete(qichepeizhi_table_path, recursive=True)
    # 在Hive中删除表
    qichepeizhi_drop_table_query = f"""DROP TABLE qichepeizhi"""
    hive_cursor.execute(qichepeizhi_drop_table_query)
    cursor.execute("SELECT * FROM qichepeizhi")
    qichepeizhi_column_info = cursor.fetchall()
    #将数据写入 CSV 文件
    with open(os.path.join(parent_directory, "qichepeizhi.csv"), 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # 写入数据行
        for row in qichepeizhi_column_info:
            writer.writerow(row)
    cursor.execute("DESCRIBE qichepeizhi")
    qichepeizhi_column_info = cursor.fetchall()
    create_table_query = "CREATE TABLE IF NOT EXISTS qichepeizhi ("
    for column, data_type, _, _, _, _ in qichepeizhi_column_info:
        match = re.match(r'(\w+)(\(\d+\))?', data_type)
        mysql_type = match.group(1)
        hive_data_type = get_hive_type(mysql_type)
        create_table_query += f"{column} {hive_data_type}, "
    qichepeizhi_create_table_query = create_table_query[:-2] + ") row format delimited fields terminated by ','"
    hive_cursor.execute(qichepeizhi_create_table_query)
    # 上传映射文件
    qichepeizhi_hdfs_csv_path = f'/user/hive/warehouse/{dbName}.db/qichepeizhi'
    hadoop_client.upload(qichepeizhi_hdfs_csv_path, os.path.join(parent_directory, "qichepeizhi.csv"))
    cursor.close()
    mysql_conn.close()
    hive_cursor.close()
    hive_conn.close()

#转换成hive的类型
def get_hive_type(mysql_type):
    type_mapping = {
        'INT': 'INT',
        'BIGINT': 'BIGINT',
        'FLOAT': 'FLOAT',
        'DOUBLE': 'DOUBLE',
        'DECIMAL': 'DECIMAL',
        'VARCHAR': 'STRING',
        'TEXT': 'STRING',
    }
    if isinstance(mysql_type, str):
        mysql_type = mysql_type.upper()
    return type_mapping.get(str(mysql_type), 'STRING')

#执行hive查询
def hive_query():
    # 连接到Hive服务器
    conn = hive.Connection(host='localhost', port=10000, username=m_username,database=dbName)
    # 创建一个游标对象
    cursor = conn.cursor()
    try:

        #定义Hive查询语句
        cheming_query = "SELECT COUNT(*) AS total, cheming FROM qichexinxi GROUP BY cheming"
        # 执行Hive查询语句
        cursor.execute(cheming_query)
        # 获取查询结果
        cheming_results = cursor.fetchall()
        cheming_json_list=[]
        for row in cheming_results:
            cheming_json_list.append({"cheming":row[1],"total":row[0]})
        #将JSON数据写入文件
        with open(os.path.join(parent_directory, "qichexinxi_groupcheming.json"), 'w', encoding='utf-8') as f:
            json.dump(cheming_json_list, f, ensure_ascii=False, indent=4)


        #定义Hive查询语句
        chebiaoqian_query = "SELECT COUNT(*) AS total, chebiaoqian FROM qichexinxi GROUP BY chebiaoqian"
        # 执行Hive查询语句
        cursor.execute(chebiaoqian_query)
        # 获取查询结果
        chebiaoqian_results = cursor.fetchall()
        chebiaoqian_json_list=[]
        for row in chebiaoqian_results:
            chebiaoqian_json_list.append({"chebiaoqian":row[1],"total":row[0]})
        #将JSON数据写入文件
        with open(os.path.join(parent_directory, "qichexinxi_groupchebiaoqian.json"), 'w', encoding='utf-8') as f:
            json.dump(chebiaoqian_json_list, f, ensure_ascii=False, indent=4)


        #定义Hive查询语句
        jiage_query = "SELECT COUNT(*) AS total, jiage FROM qichexinxi GROUP BY jiage"
        # 执行Hive查询语句
        cursor.execute(jiage_query)
        # 获取查询结果
        jiage_results = cursor.fetchall()
        jiage_json_list=[]
        for row in jiage_results:
            jiage_json_list.append({"jiage":row[1],"total":row[0]})
        #将JSON数据写入文件
        with open(os.path.join(parent_directory, "qichexinxi_groupjiage.json"), 'w', encoding='utf-8') as f:
            json.dump(jiage_json_list, f, ensure_ascii=False, indent=4)


        #定义Hive查询语句
        kuaichong_query = "SELECT COUNT(*) AS total, kuaichong FROM qichexinxi GROUP BY kuaichong"
        # 执行Hive查询语句
        cursor.execute(kuaichong_query)
        # 获取查询结果
        kuaichong_results = cursor.fetchall()
        kuaichong_json_list=[]
        for row in kuaichong_results:
            kuaichong_json_list.append({"kuaichong":row[1],"total":row[0]})
        #将JSON数据写入文件
        with open(os.path.join(parent_directory, "qichexinxi_groupkuaichong.json"), 'w', encoding='utf-8') as f:
            json.dump(kuaichong_json_list, f, ensure_ascii=False, indent=4)


        #定义Hive查询语句
        baigonglijiasu_query = "SELECT COUNT(*) AS total, baigonglijiasu FROM qichexinxi GROUP BY baigonglijiasu"
        # 执行Hive查询语句
        cursor.execute(baigonglijiasu_query)
        # 获取查询结果
        baigonglijiasu_results = cursor.fetchall()
        baigonglijiasu_json_list=[]
        for row in baigonglijiasu_results:
            baigonglijiasu_json_list.append({"baigonglijiasu":row[1],"total":row[0]})
        #将JSON数据写入文件
        with open(os.path.join(parent_directory, "qichexinxi_groupbaigonglijiasu.json"), 'w', encoding='utf-8') as f:
            json.dump(baigonglijiasu_json_list, f, ensure_ascii=False, indent=4)


        #定义Hive查询语句
        zidongjiashi_query = "SELECT COUNT(*) AS total, zidongjiashi FROM qichexinxi GROUP BY zidongjiashi"
        # 执行Hive查询语句
        cursor.execute(zidongjiashi_query)
        # 获取查询结果
        zidongjiashi_results = cursor.fetchall()
        zidongjiashi_json_list=[]
        for row in zidongjiashi_results:
            zidongjiashi_json_list.append({"zidongjiashi":row[1],"total":row[0]})
        #将JSON数据写入文件
        with open(os.path.join(parent_directory, "qichexinxi_groupzidongjiashi.json"), 'w', encoding='utf-8') as f:
            json.dump(zidongjiashi_json_list, f, ensure_ascii=False, indent=4)


        #定义Hive查询语句
        xuhang_query = "SELECT COUNT(*) AS total, xuhang FROM qichexinxi GROUP BY xuhang"
        # 执行Hive查询语句
        cursor.execute(xuhang_query)
        # 获取查询结果
        xuhang_results = cursor.fetchall()
        xuhang_json_list=[]
        for row in xuhang_results:
            xuhang_json_list.append({"xuhang":row[1],"total":row[0]})
        #将JSON数据写入文件
        with open(os.path.join(parent_directory, "qichexinxi_groupxuhang.json"), 'w', encoding='utf-8') as f:
            json.dump(xuhang_json_list, f, ensure_ascii=False, indent=4)


        #定义Hive查询语句
        dianchirongliang_query = "SELECT COUNT(*) AS total, dianchirongliang FROM qichepeizhi GROUP BY dianchirongliang"
        # 执行Hive查询语句
        cursor.execute(dianchirongliang_query)
        # 获取查询结果
        dianchirongliang_results = cursor.fetchall()
        dianchirongliang_json_list=[]
        for row in dianchirongliang_results:
            dianchirongliang_json_list.append({"dianchirongliang":row[1],"total":row[0]})
        #将JSON数据写入文件
        with open(os.path.join(parent_directory, "qichepeizhi_groupdianchirongliang.json"), 'w', encoding='utf-8') as f:
            json.dump(dianchirongliang_json_list, f, ensure_ascii=False, indent=4)


        #定义Hive查询语句
        chepeizhi_query = "SELECT COUNT(*) AS total, chepeizhi FROM qichepeizhi GROUP BY chepeizhi"
        # 执行Hive查询语句
        cursor.execute(chepeizhi_query)
        # 获取查询结果
        chepeizhi_results = cursor.fetchall()
        chepeizhi_json_list=[]
        for row in chepeizhi_results:
            chepeizhi_json_list.append({"chepeizhi":row[1],"total":row[0]})
        #将JSON数据写入文件
        with open(os.path.join(parent_directory, "qichepeizhi_groupchepeizhi.json"), 'w', encoding='utf-8') as f:
            json.dump(chepeizhi_json_list, f, ensure_ascii=False, indent=4)

        pass
    except Exception as e:
         print(f"An error occurred: {e}")
    finally:
        # 关闭游标和连接
        cursor.close()
        conn.close()

    # hive分析
def hive_analyze(request):
    if request.method in ["POST", "GET"]:
        msg = {"code": normal_code, "msg": "成功", "data": {}}
        try:
            migrate_to_hive()
            hive_query()
            return JsonResponse(msg, encoder=CustomJsonEncoder)
        except Exception as e:
            msg['code'] = system_error_code
            msg['msg'] = f"发生错误：{e}"
            return JsonResponse(msg, encoder=CustomJsonEncoder)



