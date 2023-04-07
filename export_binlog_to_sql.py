#!/usr/bin/env python
# -*- coding: utf-8 -*-
# time: 2023/4/3 17:23
# file: binlog_export_SQL.py
# author: qinxi
# email: 1023495336@qq.com
import os.path
import re
import subprocess

def Parsing_Binlog(start_datetime,stop_datetime,database,binlog_filename):
    '''
    :param start_datetime: "2023-04-01 11:36:00"
    :param stop_datetime: "2023-04-01 11:40:00"
    :param database: "db_bulv_chao"
    :param binlog_filename: "mysql-bin.000007"
    :return: "./binlog-export.sql"
    '''
    output_file = "./binlog-export.sql"

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

    if os.path.exists(binlog_filename):
        print(f'正在解析{binlog_filename}......')
        # 执行命令行命令
        subprocess.run(args)
        print('已解析binlog日志文件到：', output_file)
    else:
        exit(f'{binlog_filename} 文件不存在')


def Export_Restore_Sql(binlog):
    # 读取导出的日志文件
    print(f'开始解析日志{binlog}.....')
    try:
        # Open the input file
        with open(binlog, 'r') as f:
            content = f.read()

        # Extract lines that contain '###'
        pattern = re.compile('.*###.*\n')
        matches = pattern.findall(content)

        # Process the matching lines
        output = []
        for match in matches:
            # Remove '### '
            line = match.replace('### ', '')
            # Remove comments
            line = re.sub(r'\/\*.*?\*\/', ',', line)
            # Replace 'DELETE FROM' with ';INSERT INTO'
            line = line.replace('DELETE FROM', ';INSERT INTO')
            # Replace 'WHERE' with 'SELECT'
            line = line.replace('WHERE', 'SELECT')
            # Replace '@17' with '@17;'
            line = re.sub(r'(@17.*),', r'\1;', line)
            # Remove '@1='
            line = line.replace('@1=', '')
            # Remove '@[1-9]='
            line = re.sub(r'@[1-9]=', ',', line)
            # Remove '@[1-9][0-9]='
            line = re.sub(r'@[1-9][0-9]=', ',', line)
            # Remove ';'
            #line = line.replace(';', '')
            output.append(line)

        sql = './mysql-logOK.sql'
        # Write the output to a file
        with open(f'{sql}', 'w') as f:
            f.write('\n'.join(output))
        print(f'{sql} 写入成功！')

    except Exception as e:
        print(f'Error occurred: {e}')


export_sql = './binlog-export.sql'

# 执行此程序时，需要修改Parsing_Binlog函数中所需的四个参数，运行后可得可执行的SQL
Parsing_Binlog("2023-04-01 11:36:00", "2023-04-01 11:40:00", "db_bulv_chao","/root/mysql-bin.001129")
size = os.path.getsize(export_sql)
if size == 0:
   exit('binlog解析文件为空！')
else:
   Export_Restore_Sql(export_sql)

if not os.path.exists(export_sql):
   exit('可执行SQL解析失败，请检查！')

