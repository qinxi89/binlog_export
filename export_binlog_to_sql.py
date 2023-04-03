#!/usr/bin/env python
# -*- coding: utf-8 -*-
# time: 2023/4/3 17:23
# file: binlog_export_SQL.py
# author: qinxi
# email: 1023495336@qq.com
import os.path
import re
import subprocess
import time


def Parsing_Binlog(start_datetime,stop_datetime,database,binlog_filename):
    '''
    :param start_datetime: "2023-04-01 11:36:00"
    :param stop_datetime: "2023-04-01 11:40:00"
    :param database: "db_bulv_chao"
    :param binlog_filename: "mysql-bin.000007"
    :return: "./binlog-export.sql"
    '''

    output_file = "./binlog-export.sql"

    if not os.path.exists(os.path.dirname(binlog_filename)):
        exit(f'{binlog_filename} 文件不存在')

    # 构造命令行参数
    args = [
        "mysqlbinlog",
        "-v",
        "--base64-output=decode-rows",
        f"--start-datetime={start_datetime}",
        f"--stop-datetime={stop_datetime}",
        f"--database={database}",
        binlog_filename,
        "-r",
        output_file,
    ]


    print(f'正在解析{binlog_filename}......')
    # 执行命令行命令
    subprocess.run(args)


def Export_Restore_Sql(binlog):
    # 读取导出的日志文件
    print(f'开始解析日志{binlog}.....')
    with open(binlog, 'r',encoding='utf-8') as f:
        log_file = f.read()

    # 进行必要的筛选和转换以生成可执行的SQL语句
    insert_statements = []
    for statement in re.findall(r'/\*.*\*/;\n(###.*?);', log_file, re.DOTALL):
        statement = re.sub(r'/\*.*?\*/', '', statement)
        statement = statement.replace('DELETE FROM', 'INSERT INTO').replace('WHERE', 'SELECT')
        statement = re.sub(r'(@\d+=)', '', statement)
        statement = re.sub(r'(@\d+=)', '', statement)
        statement = re.sub(r'(@\d+=)', '', statement)
        statement = re.sub(r'(@\d+=)', '', statement)
        statement = re.sub(r'(@\d+=)', '', statement)
        statement = re.sub(r'(@\d+=)', '', statement)
        statement = re.sub(r'(@\d+=)', '', statement)
        statement = re.sub(r'(@\d+=)', '', statement)
        statement = re.sub(r',(\n|\s)*\)', ')', statement)
        insert_statements.append(statement.strip() + ';')

    print(f'正在打开{binlog}')
    # 将生成的SQL语句写入到一个新的SQL文件中，该文件可以用于恢复数据库
    with open('mysqllogOK.sql', 'w') as f:
        f.write('\n'.join(insert_statements))

    print('mysqllogOK.sql 写入成功！')


Parsing_Binlog("2023-04-01 11:36:00", "2023-04-01 11:40:00", "db_bulv_chao","./mysql-bin.001129")

size = os.path.getsize('./binlog-export.sql')
if size == 0:
    exit('binlog-export.sql 为空，解析失败')
else:
    print('im here')
    Export_Restore_Sql('./binlog-export.sql')
    
    
    
