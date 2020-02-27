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

srcuser = os.environ['SRCUSER'] 
srcpass = os.environ['SRCPASS'] 

dsn = """(DESCRIPTION=
             (FAILOVER=on)
             (ADDRESS_LIST=
               (ADDRESS=(PROTOCOL=tcp)(HOST=krtndev2-scan.pitagoras.apollo.br)(PORT=1521)))
             (CONNECT_DATA=(SERVICE_NAME=ora44)))"""

def connect_Pool():

    try:

        # Create the session pool
        pool = cx_Oracle.SessionPool(srcuser, srcpass, dsn , min=2, max=5, increment=1, encoding="UTF-8") # "dbhost.example.com/orclpdb1"

        # Acquire a connection from the pool
        connection = pool.acquire()

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

    return json.dumps(json_data, default = json_converter)

   

def valida_python_version():
    if not sys.version_info > (2, 7):
        # berate your user for running a 10 year
        # python version
        print('>>> INFO: [valida_python_version] This is not work on this python version, you need to upgrade to >= 3.6')
        exit(4)
    elif not sys.version_info >= (3, 5):
        # Kindly tell your user (s)he needs to upgrade
        # because you're using 3.5 features
        print('>>> INFO: I strongly recommend that you update the python version to >= 3.6')
        exit(5)   


def valida_usuario_senha():
    if None in (os.environ['SRCUSER'],os.environ['SRCPASS']):
        print('>>> INFO: [valida_usuario_senha] User and pass not set')
        exit(6)


def json_converter(o):
    if isinstance(o, datetime.datetime):
        if '00:00:00' in o.strftime("%H:%M:%S"):
            return o.strftime("%d/%m/%Y")
        else:
            return o.strftime("%d/%m/%Y %H:%M:%S")


def main():

    resultado = {}

    try:

        valida_python_version()

        # Create the connection and open the pool
        pool,connection = connect_Pool()

        resultado = execute_Pool(connection,"select alucod,espcod,tmacod,inscod,acrdataingresso from rw_olimpo.aluncurs where rownum < 3")
        print(resultado)

        # Close the pool and connection
        disconnect_Pool(pool,connection)

    except Exception as ex:

        print(">>> ERROR: [main] %s [] %s" % (ex,traceback.format_exc()))
        exit(7)


main()