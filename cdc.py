# -*- coding: utf-8 -*-

import cx_Oracle
import sys
import os
import traceback
#import json
from datetime import datetime, timedelta
from time import sleep
from json import dumps, loads
from kafka import KafkaProducer, KafkaConsumer


'''
export SRCUSER="sys"
export SRCPASS="Oradoc_dbl"

export SRCUSER="PRODUCAO"
export SRCPASS="producao"
'''
source = 'OLIMPO'
source_host = '10.63.38.247'
source_port = 32769
source_service_name = 'ORCLCDB.localdomain'
source_owner = 'PRODUCAO'
source_table = 'ALUNCURS'

source_database_user = None
source_database_pass = None
cursor = None
connection = None

kafka_host = 'localhost:9092'
topic_control = 'OLIMPO-ALUNCURS-CONTROLE'
topic = 'OLIMPO-ALUNCURS'
data_control_json = { "START_TIME": "[START_TIME]" , "END_TIME": "[END_TIME]" }
logminer_start = "BEGIN DBMS_LOGMNR.START_LOGMNR(startTime => TO_DATE([START_TIME], 'DD/MM/YYYY HH24:MI:SS'),endTime => TO_DATE([END_TIME], 'DD/MM/YYYY HH24:MI:SS'),OPTIONS => DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.CONTINUOUS_MINE + DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;"
logminer_query = ("SELECT OPERATION_CODE,OPERATION,COMMIT_TIMESTAMP,SEG_TYPE_NAME,SEG_OWNER,TABLE_NAME,TABLE_SPACE,USERNAME,ROW_ID,SQL_REDO,SQL_UNDO FROM V$LOGMNR_CONTENTS WHERE TABLE_NAME = '%s' AND SEG_OWNER = '%s' AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE') ORDER BY SCN" % (source_table,source_owner))
logminer_end = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;"

sample_data = """
                { "OPERATION_CODE": "U",
                "OPERATION" : "UPDATE",
                "COMMIT_TIMESTAMP" : "123",
                "SEG_TYPE_NAME" : "REG",
                "SEG_OWNER" : "PRODUCAO",
                "TABLE_NAME" : "TABELA",
                "TABLE_SPACE" : " ",
                "USERNAME" : "USUARIO",
                "ROW_ID" : "ASHAGSJHAGSGJAGS",
                "SQL_REDO" : "UPDATE TMACOD='teste' where ID = 1",
                "SQL_UNDO" : "UPDATE TMACOD='anterior' where ID = 1"
                }
              """


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

    global source_database_user,source_database_pass

    if 'SRCUSER' not in os.environ or 'SRCPASS' not in os.environ:
        print(">>> ERROR: [valida_usuario_senha] Enviroment variables not set, exiting now... ")
        exit(5)

        if None in (os.environ['SRCUSER'],os.environ['SRCPASS']):
            print('>>> ERROR: [valida_usuario_senha] User name and password not set, exiting now...')
            exit(5)

    source_database_user = os.environ['SRCUSER'] 
    source_database_pass = os.environ['SRCPASS'] 

    print('>>> INFO: Using user %s' % source_database_user)

def json_converter(o):
    if isinstance(o, datetime.datetime):
        if '00:00:00' in o.strftime("%H:%M:%S"):
            return o.strftime("%d/%m/%Y")
        else:
            return o.strftime("%d/%m/%Y %H:%M:%S")

def create_Producer(kafka_host):
    return KafkaProducer(bootstrap_servers=[kafka_host],value_serializer=lambda x: dumps(x).encode('utf-8'),key_serializer=lambda x: dumps(x).encode('utf-8'))

def create_Consumer(kafka_host,topic,group_id):
    return KafkaConsumer(topic,
        bootstrap_servers=[kafka_host],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=5000)

def execute(data):

    print(data_control_json)

    delta=[]

    # # Create the connection
    # ##################################################################################            
    # dsn = cx_Oracle.makedsn(source_host, source_port,service_name=source_service_name)
    # connection = cx_Oracle.connect(source_database_user, source_database_pass, dsn , encoding="UTF-8",mode=cx_Oracle.SYSDBA)

    # # Open Cursor
    # cursor = connection.cursor()
    # cursor.execute("alter session set nls_date_format = 'DD/MM/YYYY HH24:MI:SS'")

    # # Starta o logminer para o intervalo de data que se quer procurar
    # print(">>> INFO: Executing DBMS_LOGMNR.START_LOGMNR")
    # start_consulta = logminer_start.replace('[START_TIME]',data_control['START_TIME'])
    # start_consulta = start_consulta.replace('[END_TIME]',data_control['END_TIME'])
    # cursor.execute(start_consulta)

    # # Executa a consulta no logminer
    # print(">>> INFO: Executing The query on V$LOGMNR_CONTENTS")
    # cursor.execute(logminer_query)
    # registros = cursor.fetchall()

    # delta=[]

    # # Pega o nome das colunas da consulta
    # colunas = [linha[0] for linha in cursor.description]

    # # Para cada registro adiciona na lista
    # for registro in registros:
    #     delta.append(dict(zip(colunas,registro)))

    # print(json.dumps(delta, default = json_converter, indent=4))

    # # Encerra o logminer
    # print(">>> INFO: Executing DBMS_LOGMNR.END_LOGMNR")
    # cursor.execute(logminer_end)

    delta.append(sample_data.replace('\n',' '))

    # PRODUCER
    ##################################################################################
    producer = create_Producer(kafka_host)

    # ROTINA PARA PUBLICAR CADA MENSAGEM NO TOPICO PRINCIPAL
    # FAZER LACO PARA delta
    print('Enviando mensagens para o topico principal',topic)
    print('   Mensagens para enviar:',len(delta))
    for i, comando in enumerate(delta):
        data = dumps(delta, default = json_converter, indent=4)
        key = str(datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')[:-3])
        #print(data)
        producer.send(topic, value=data)
        print('   Mensagem %s enviada!' % str(i+1))
        sleep(0.01)

    print('Atualizando o topico de controle',topic_control)
    # Produz mensagem no topico de controle com a data da ultima leitura
    producer.send(topic_control, value=data_control_json)
    producer.close()                 


def main():

    # Validacoes
    ###################################################################################    
    # python version
    #valida_python_version()

    # user and password of source
    #valida_usuario_senha()

    global cursor, connection, logminer_start, logminer_query, logminer_end, topic_control, data_control_json
    resultado = {}

    try:

        # CONSUMER para topico de controle
        ##################################################################################    
        consumer = create_Consumer(kafka_host,topic_control,topic_control)

        #print(consumer)
        #input('Tecle para continuar...')

        # Verifica a ultima data de atualizacao
        messages = []

        for message in consumer:
            message = message.value
            messages.append(message)
            consumer.commit()

        consumer.close()

        #print(messages)
        #input('lista das mensagens - Tecle para continuar...')

        # Se não houver nenhuma data
        if len(messages) == 0:
            data_control_json['START_TIME'] = '{0:%d/%m/%Y %H:%M:%S}'.format(datetime.now() - timedelta(minutes=15))
            data_control_json['END_TIME'] = '{0:%d/%m/%Y %H:%M:%S}'.format(datetime.now())
            execute(data_control_json)
        # Se houver pega a data dela
        else:
            for message in messages:
                data_control_json['START_TIME'] = message['END_TIME']
                data_control_json['END_TIME'] = '{0:%d/%m/%Y %H:%M:%S}'.format(datetime.now())
                execute(data_control_json)
                

    except Exception as ex:
        print(">>> ERROR: %s [] %s" % (ex,traceback.format_exc()))
        exit(1)

    #finally:
        # # Close Cursor
        # cursor.close()

        # # Close connection
        # connection.close()
        # 

 
             



main()