# -*- coding: utf-8 -*-

import cx_Oracle
import threading
from traceback import format_exc
from os import environ
from sys import version_info
from datetime import datetime, timedelta
from time import sleep
from json import dumps, loads
from kafka import KafkaProducer, KafkaConsumer
import logging


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

target_host_kafka = 'localhost:9092'
target_topic_control = 'CONTROLE-OLIMPO-ALUNCURS'
target_topic = 'OLIMPO-ALUNCURS'

source_database_user = None
source_database_pass = None


# Processamento das mensagens
class MyProcessingThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def create_Consumer(target_host_kafka,target_topic,group_id):
        return KafkaConsumer(target_topic,
            bootstrap_servers=[target_host_kafka],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: loads(x.decode('utf-8')),
            consumer_timeout_ms=5000)

    def create_Producer(target_host_kafka):
        return KafkaProducer(bootstrap_servers=[target_host_kafka],value_serializer=lambda x: dumps(x).encode('utf-8'),key_serializer=lambda x: dumps(x).encode('utf-8'))

    def json_converter(o):
        if isinstance(o, datetime.datetime):
            if '00:00:00' in o.strftime("%H:%M:%S"):
                return o.strftime("%d/%m/%Y")
            else:
                return o.strftime("%d/%m/%Y %H:%M:%S")

    def run(self):

        global source, source_host, source_port, source_service_name, source_owner, source_table

        logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Iniciando Thread...')

        cursor = None
        connection = None

        data_control_json = { "START_TIME": "[START_TIME]" , "END_TIME": "[END_TIME]" }
        logminer_start = "BEGIN DBMS_LOGMNR.START_LOGMNR(startTime => TO_DATE([START_TIME], 'DD/MM/YYYY HH24:MI:SS'),endTime => TO_DATE([END_TIME], 'DD/MM/YYYY HH24:MI:SS'),OPTIONS => DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.CONTINUOUS_MINE + DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;"
        logminer_query = ("SELECT OPERATION_CODE,OPERATION,COMMIT_TIMESTAMP,SEG_TYPE_NAME,SEG_OWNER,TABLE_NAME,TABLE_SPACE,USERNAME,ROW_ID,SQL_REDO,SQL_UNDO FROM V$LOGMNR_CONTENTS WHERE TABLE_NAME = '%s' AND SEG_OWNER = '%s' AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE') ORDER BY SCN" % (source_table,source_owner))
        logminer_end = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;"

        resultado = {}

        try:

            print('>>> INFO: Iniciando execução...',str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')))

            # CONSUMER para topico de controle
            ##################################################################################
            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Creating consumer...')
            consumer = self.create_Consumer(target_host_kafka,target_topic_control,target_topic_control)

            #print(consumer)
            #input('Tecle para continuar...')

            # Verifica a ultima data de atualizacao
            messages = []

            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Reading messages...')
            for message in consumer:
                message = message.value
                messages.append(message)
                consumer.commit()

            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Closing consumer...')
            consumer.close()

            #print(messages)
            #input('lista das mensagens - Tecle para continuar...')

            # Se não houver nenhuma data
            if len(messages) == 0:
                data_control_json['START_TIME'] = '{0:%d/%m/%Y %H:%M:%S}'.format(datetime.now() - timedelta(minutes=15))
                data_control_json['END_TIME'] = '{0:%d/%m/%Y %H:%M:%S}'.format(datetime.now())
            # Se houver pega a data dela
            else:
                for message in messages:
                    data_control_json['START_TIME'] = message['END_TIME']
                    data_control_json['END_TIME'] = '{0:%d/%m/%Y %H:%M:%S}'.format(datetime.now())

            # Execute
            # print(data_control_json)

            delta=[]

            # Create the connection
            ##################################################################################            
            dsn = cx_Oracle.makedsn(source_host, source_port,service_name=source_service_name)
            connection = cx_Oracle.connect(source_database_user, source_database_pass, dsn , encoding="UTF-8",mode=cx_Oracle.SYSDBA)

            # Open Cursor
            cursor = connection.cursor()
            cursor.execute("alter session set nls_date_format = 'DD/MM/YYYY HH24:MI:SS'")

            # Starta o logminer para o intervalo de data que se quer procurar
            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Executando DBMS_LOGMNR.START_LOGMNR...')
            print(">>> INFO: Executando DBMS_LOGMNR.START_LOGMNR")
            start_consulta = logminer_start.replace('[START_TIME]',data_control_json['START_TIME'])
            start_consulta = start_consulta.replace('[END_TIME]',data_control_json['END_TIME'])
            cursor.execute(start_consulta)

            # Executa a consulta no logminer
            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Executando a consulta em V$LOGMNR_CONTENTS...')
            print(">>> INFO: Executando a consulta em V$LOGMNR_CONTENTS")
            cursor.execute(logminer_query)
            registros = cursor.fetchall()

            delta=[]

            # Pega o nome das colunas da consulta
            colunas = [linha[0] for linha in cursor.description]

            # Para cada registro adiciona na lista
            for registro in registros:
                delta.append(dict(zip(colunas,registro)))

            #print(json.dumps(delta, default = json_converter, indent=4))

            # Encerra o logminer
            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Executando DBMS_LOGMNR.END_LOGMNR...')
            print(">>> INFO: Executing DBMS_LOGMNR.END_LOGMNR")
            cursor.execute(logminer_end)

            #delta.append(sample_data.replace('\n',' '))

            # PRODUCER
            ##################################################################################
            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Creating producer...')
            producer = self.create_Producer(target_host_kafka)

            # ROTINA PARA PUBLICAR CADA MENSAGEM NO TOPICO PRINCIPAL
            # FAZER LACO PARA delta
            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Sending messages to main topic '+target_topic)
            print('>>> INFO: Enviando mensagens para o topico principal',target_topic)
            print('   Mensagens para enviar:',len(delta))
            for i, comando in enumerate(delta):
                data = dumps(delta, default = self.json_converter, indent=4)
                key = str(datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')[:-3])
                #print(data)
                producer.send(target_topic, value=data)
                print('   Mensagem %s enviada!' % str(i+1))
                sleep(0.01)

            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Updating topic control '+target_topic_control)
            print('>>> INFO: Atualizando o topico de controle',target_topic_control)
            # Produz mensagem no topico de controle com a data da ultima leitura
            producer.send(target_topic_control, value=data_control_json)

            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Closing producer...')
            producer.close()  

        except Exception as ex:
            logging.error(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' ' + ex + ' ' + format_exc())
            print(">>> ERROR: %s [] %s" % (ex,format_exc()))
            exit(1)

def valida_python_version():
    if not version_info > (2, 7):
        # berate your user for running a 10 year
        # python version
        logging.error(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' This is not work on this python version, you need to upgrade to >= 3.6')
        print('>>> ERROR: [valida_python_version] This is not work on this python version, you need to upgrade to >= 3.6')
        exit(4)
    elif not version_info >= (3, 5):
        # Kindly tell your user (s)he needs to upgrade
        # because you're using 3.5 features
        logging.error(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' I strongly recommend that you update the python version to >= 3.6')
        print('>>> ERROR: [valida_python_version] I strongly recommend that you update the python version to >= 3.6')
        exit(4)  
    logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Passou pela validação de versão python')

def valida_usuario_senha():

    global source_database_user,source_database_pass

    if 'SRCUSER' not in environ or 'SRCPASS' not in environ:
        logging.error(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Enviroment variables not set')
        print(">>> ERROR: Enviroment variables not set.")
        exit(5)

        if None in (environ['SRCUSER'],environ['SRCPASS']):
            logging.error(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Enviroment variables not set')
            print('>>> ERROR: User name and password not set.')
            exit(5)

    source_database_user = environ['SRCUSER'] 
    source_database_pass = environ['SRCPASS'] 
    
    logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Source user ' + source_database_user)
    print('>>> INFO: Using user %s' % source_database_user)


LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}


# if len(sys.argv) > 1:
#     level_name = sys.argv[1]
#     level = LEVELS.get(level_name, logging.NOTSET)
#     logging.basicConfig(level=level)


logging.basicConfig(filename='cdc.log',level=logging.DEBUG)
logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Aplicativo inicializado')


# Validacoes
###################################################################################    
# python version
valida_python_version()

# user and password of source
valida_usuario_senha()

error_count = 0
error_max = 5
while True:

    try:
        
        if error_count == 5:
            logging.error(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Error limit exceeded')
            print(">>> ERROR: Limite de erros excedido!")
            exit(9)
        
        task = MyProcessingThread()
        task.start()
        task.join()

        waiting_time = 10
        print('>>> INFO: Aguardando',str(waiting_time),'para próxima execução\n')
        logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Aguardando '+str(waiting_time)+' segundos para próxima execução')
        sleep(waiting_time)

        exit(10)

    except Exception as ex:
        logging.error(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' ' + ex + ' ' + format_exc())
        print(">>> ERROR: %s [] %s" % (ex,format_exc()))
        sleep(20)
        error_count += 1

    except KeyboardInterrupt as it:
        logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' CTRL+C acionado')
        print("\n>>> INFO: Ctrl+C acionado, encerrando...")
        exit(2)

