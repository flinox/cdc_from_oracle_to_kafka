# -*- coding: utf-8 -*-

import cx_Oracle
import sys
import os
import traceback
import json
import datetime

'''
export SRCUSER="sys"
export SRCPASS="Oradoc_dbl"

export SRCUSER="PRODUCAO"
export SRCPASS="producao"
'''

srcuser = None
srcpass = None

dsn = """(DESCRIPTION=
             (FAILOVER=on)
             (ADDRESS_LIST=
               (ADDRESS=(PROTOCOL=tcp)(HOST=10.63.38.247)(PORT=32769)))
             (CONNECT_DATA=(SERVICE_NAME=ORCLCDB.localdomain)))"""

def connect_Pool():

    try:

        #dsn = cx_Oracle.makedsn('host', port, 'sid')

        # Create the session pool
        pool = cx_Oracle.SessionPool(srcuser, srcpass, dsn , min=2, max=5, increment=1, encoding="UTF-8",connectiontype=cx_Oracle.Connection,getmode=cx_Oracle.SYSDBA) # "dbhost.example.com/orclpdb1"

        # Acquire a connection from the pool
        connection = pool.acquire()
        #connection.clientinfo = 'python 2.6.4 - win32'
        #connection.module = 'cx_Oracle SessionPool demo'
        #connection.action = 'Get sysdate'

    except Exception as ex:
        print(">>> ERROR: [connect_Pool] %s [] %s" % (ex,traceback.format_exc()))
        exit(1)

    return pool,connection

def disconnect_Pool(pool,connection):

    try:

        # Release the connection to the pool
        pool.release(connection)

        # Close the pool
        pool.close()

    except Exception as ex:

        print(">>> ERROR: [disconnect_Pool] %s [] %s" % (ex,traceback.format_exc()))
        exit(2)

def execute_Pool(connection,query):

    json_data=[]

    try:

        # Use the pooled connection
        cursor = connection.cursor()
        cursor.execute(query)
        colunas = [linha[0] for linha in cursor.description]
        registros = cursor.fetchall()

        for registro in registros:
            json_data.append(dict(zip(colunas,registro)))

    except Exception as ex:
        print(">>> ERROR: [execute_Pool] %s [] %s" % (ex,traceback.format_exc()))
        exit(3)

    return json.dumps(json_data, default = json_converter, indent=4)

def execute_Pool_Statement(connection,query):

    try:

        # Use the pooled connection
        cursor = connection.cursor()
        cursor.execute(query)
        return True

    except Exception as ex:
        print(" >>> ERROR: [execute_Pool] %s [] %s" % (ex,traceback.format_exc()))

        return False

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

    resultado = {}

    try:

        # Validate python version
        valida_python_version()

        # Validate user and password
        valida_usuario_senha()

        # Create the connection and open the pool
        pool,connection = connect_Pool()

        print(">>> INFO: Executing Alter session")
        if execute_Pool_Statement(connection,"alter session set nls_date_format = 'DD/MM/YYYY HH24:MI:SS'"):
            
            print(">>> INFO: Executing DBMS_LOGMNR.START_LOGMNR")
            if execute_Pool_Statement(connection,"BEGIN DBMS_LOGMNR.START_LOGMNR(startTime => TO_DATE('28/01/2020 12:20:42', 'DD/MM/YYYY HH24:MI:SS')," +
                                                 " endTime => TO_DATE(SYSDATE, 'DD/MM/YYYY HH24:MI:SS')," +
                                                 " OPTIONS => DBMS_LOGMNR.COMMITTED_DATA_ONLY +" +
                                                 " DBMS_LOGMNR.CONTINUOUS_MINE +" +
                                                 " DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG" +
                                                 " ); END;"):
                print(">>> INFO: Executing The query on V$LOGMNR_CONTENTS")
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
                                WHERE TABLE_NAME = 'PRODUCAO'
                                AND SEG_OWNER = 'ALUNCURS'
                                AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
                                ORDER BY SCN""" #.replace('\n',' ')
                print(consulta)
                resultado = execute_Pool(connection,consulta)
                print(resultado)

                if execute_Pool_Statement(connection,"BEGIN DBMS_LOGMNR.END_LOGMNR; END;"):
                    pass

        # Close the pool and connection
        disconnect_Pool(pool,connection)

    except Exception as ex:

        print(">>> ERROR: [main] %s [] %s" % (ex,traceback.format_exc()))
        exit(6)


main()