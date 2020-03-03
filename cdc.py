# -*- coding: utf-8 -*-

import cx_Oracle
import sys
import os
import traceback
import json
import datetime
import time

'''
export SRCUSER="sys"
export SRCPASS="Oradoc_dbl"

export SRCUSER="PRODUCAO"
export SRCPASS="producao"
'''

srcuser = None
srcpass = None
cursor = None
connection = None

start = "BEGIN DBMS_LOGMNR.START_LOGMNR(startTime => TO_DATE('02/03/2020 12:00:00', 'DD/MM/YYYY HH24:MI:SS'),endTime => TO_DATE('02/03/2020 18:50:00', 'DD/MM/YYYY HH24:MI:SS'),OPTIONS => DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.CONTINUOUS_MINE + DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;"
consulta = """SELECT    OPERATION_CODE,
                        OPERATION,
                        COMMIT_TIMESTAMP,
                        SEG_TYPE_NAME,
                        SEG_OWNER,
                        TABLE_NAME,
                        TABLE_SPACE,
                        USERNAME,
                        ROW_ID,
                        SQL_REDO,
                        SQL_UNDO
                FROM V$LOGMNR_CONTENTS
                WHERE TABLE_NAME = 'ALUNCURS'
                AND SEG_OWNER = 'PRODUCAO'
                AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
                ORDER BY SCN""".replace('\n',' ')
end = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;"

def valida_python_version():
    if not sys.version_info > (2, 7):
        # berate your user for running a 10 year
        # python version
        print('>>> ERROR: [valida_python_version] This is not work on this python version, you need to upgrade to >= 3.6')
        exit(4)
    elif not sys.version_info >= (3, 5):
        # Kindly tell your user (s)he needs to upgrade
        # because you're using 3.5 features
        print('>>> ERROR: I strongly recommend that you update the python version to >= 3.6')
        exit(4)   

def valida_usuario_senha():

    global srcuser,srcpass

    if 'SRCUSER' not in os.environ or 'SRCPASS' not in os.environ:
        print(">>> ERROR: [valida_usuario_senha] Enviroment variables not set, exiting now... ")
        exit(5)

        if None in (os.environ['SRCUSER'],os.environ['SRCPASS']):
            print('>>> ERROR: [valida_usuario_senha] User name and password not set, exiting now...')
            exit(5)

    srcuser = os.environ['SRCUSER'] 
    srcpass = os.environ['SRCPASS'] 

    print('>>> INFO: Using user %s' % srcuser)

def json_converter(o):
    if isinstance(o, datetime.datetime):
        if '00:00:00' in o.strftime("%H:%M:%S"):
            return o.strftime("%d/%m/%Y")
        else:
            return o.strftime("%d/%m/%Y %H:%M:%S")


def main():

    global cursor, connection, start, consulta, end
    resultado = {}

    try:

        # Validate python version
        valida_python_version()

        # Validate user and password
        valida_usuario_senha()

        # Create the connection
        dsn = cx_Oracle.makedsn('10.63.38.247', 32769,service_name='ORCLCDB.localdomain')

        # Acquire a connection from the pool
        connection = cx_Oracle.connect(srcuser, srcpass, dsn , encoding="UTF-8",mode=cx_Oracle.SYSDBA)
        #connection.autocommit = True

        # Open Cursor
        cursor = connection.cursor()

        print(">>> INFO: Executing Alter session")
        cursor.execute("alter session set nls_date_format = 'DD/MM/YYYY HH24:MI:SS'")

        print(">>> INFO: Executing DBMS_LOGMNR.START_LOGMNR")
        cursor.execute(start)

        print(">>> INFO: Executing The query on V$LOGMNR_CONTENTS")
        registros = cursor.execute(consulta)
        registros = cursor.fetchall()

        json_data=[]

        try:

            colunas = [linha[0] for linha in cursor.description]

            for registro in registros:
                json_data.append(dict(zip(colunas,registro)))

        except Exception as ex:
            print(">>> ERROR: [consulta] %s [] %s" % (ex,traceback.format_exc()))
            exit(3)

        print(json.dumps(json_data, default = json_converter, indent=4))

        print(">>> INFO: Executing DBMS_LOGMNR.END_LOGMNR")
        cursor.execute(end)

        # Tempo
        print('Waiting 3 seconds...')
        time.sleep(3)

        # Close Cursor
        cursor.close()

        # Close connection
        connection.close()

    except Exception as ex:

        print(">>> ERROR: [main] %s [] %s" % (ex,traceback.format_exc()))
        exit(6)


main()