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
import argparse
import textwrap


# Processamento das mensagens
class MyProcessingThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def create_Consumer(self,target_host_kafka,target_topic,group_id,timeout):
        return KafkaConsumer(target_topic,
            bootstrap_servers=[target_host_kafka],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: loads(x.decode('utf-8')),
            consumer_timeout_ms=timeout)

    def create_Producer(self,target_host_kafka):
        return KafkaProducer(bootstrap_servers=[target_host_kafka],value_serializer=lambda x: dumps(x).encode('utf-8'),key_serializer=lambda x: dumps(x).encode('utf-8'))

    def json_converter(self,o):
        if isinstance(o, datetime):
            if '00:00:00' in o.strftime("%H:%M:%S"):
                return o.strftime("%d/%m/%Y")
            else:
                return o.strftime("%d/%m/%Y %H:%M:%S")

    def run(self):

        global source, source_host, source_port, source_service_name, source_owner, source_table, consumer_timeout

        logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Iniciando Thread...')

        cursor = None
        connection = None

        data_control_json = { "START_TIME": "[START_TIME]" , "END_TIME": "[END_TIME]" }
        logminer_start = "BEGIN DBMS_LOGMNR.START_LOGMNR(startTime => TO_DATE('[START_TIME]', 'DD/MM/YYYY HH24:MI:SS'),endTime => TO_DATE('[END_TIME]', 'DD/MM/YYYY HH24:MI:SS'),OPTIONS => DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.CONTINUOUS_MINE + DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;"
        logminer_query = ("SELECT OPERATION,COMMIT_TIMESTAMP,SEG_OWNER,TABLE_NAME,ROW_ID,SQL_REDO,SQL_UNDO FROM V$LOGMNR_CONTENTS WHERE TABLE_NAME = '%s' AND SEG_OWNER = '%s' AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE') ORDER BY SCN" % (source_table,source_owner))
        logminer_end = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;"

        resultado = {}

        try:

            print('>>> INFO: Iniciando execução...',str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')))

            # CONSUMER para topico de controle
            ##################################################################################
            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Creating consumer...')
            consumer = self.create_Consumer(target_host_kafka,target_topic_control,target_topic_control,consumer_timeout)

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
                data_control_json['START_TIME'] = '{0:%d/%m/%Y %H:%M:%S}'.format( ( datetime.now() - timedelta(minutes=15) ) )
                data_control_json['END_TIME'] = '{0:%d/%m/%Y %H:%M:%S}'.format(datetime.now())
            # Se houver pega a data dela
            else:
                for message in messages:
                    data_control_json['START_TIME'] = message['END_TIME']
                    data_control_json['END_TIME'] = '{0:%d/%m/%Y %H:%M:%S}'.format(datetime.now())

            print('>>> INFO: Intervalo: ',data_control_json['START_TIME'],'e',data_control_json['END_TIME'])

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

            # TEMP
            #cursor.execute("alter session set TIME_ZONE = '+3:00'")
            # TEMP

            # Starta o logminer para o intervalo de data que se quer procurar
            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Executando DBMS_LOGMNR.START_LOGMNR...')
            print(">>> INFO: Executando DBMS_LOGMNR.START_LOGMNR")

            start_consulta = logminer_start.replace('[START_TIME]',data_control_json['START_TIME'])
            start_consulta = start_consulta.replace('[END_TIME]',data_control_json['END_TIME'])
            
            #print(start_consulta)
            #exit(8)

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
            print('    Mensagens para enviar:',len(delta))
            for i, comando in enumerate(delta):
                data = dumps(delta, default = self.json_converter, indent=4)
                key = str(datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')[:-3])
                #print(data)
                producer.send(target_topic, value=data)
                print('    Mensagem %s enviada!' % str(i+1))
                sleep(0.01)

            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Updating topic control '+target_topic_control)
            print('>>> INFO: Atualizando o topico de controle',target_topic_control)
            # Produz mensagem no topico de controle com a data da ultima leitura
            producer.send(target_topic_control, value=data_control_json)

            logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Closing producer...')
            producer.close()  

        except Exception as ex:
            logging.error(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' ' + format_exc())
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

source_database_user = None
source_database_pass = None

# Define os parametros esperados e obrigatorios
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,description=textwrap.dedent('''\
         CDC FROM ORACLE TO KAFKA 
         ----------------------------------------------------------------------------------------
         Created By: 
            Fernando Lino Di Tomazzo Silva ( https://www.linkedin.com/in/flinox )
            Fernanda Miola Titato ( https://www.linkedin.com/in/fernanda-miola-titato-a3471224 )

         Version 1.0 - 2020-03-06 

         Sintaxe: 
         python cdc.py SISTEMA 10.63.38.247 1521 ORCLCDB.localdomain OWNER TABLE localhost:9092

         Obs.:
         É preciso informar as variaveis de ambiente SRCUSER e SRCPASS para acesso ao banco de origem da conexao que será criada.
         O Topico no kafka será criado usando o seguinte padrao [source]-[source_table]
         O Topico de controle do intervalo de data processado será criado usando o seguinte padrao CONTROLE-[source]-[source_table]
         Se não conseguir buscar o intervalo de execucao do topico de controle, o programa irá pegar os ultimos 15 minutos

         '''))


parser.add_argument("source", help="Nome de identificacao do sistema de origem, será usado para organizar as mensagens no nome do topico no kafka")
parser.add_argument("source_host", help="Informe o nome ou IP do banco de dados oracle de origem")
parser.add_argument("source_port", help="Informe a porta do oracle do banco de dados oracle de origem")
parser.add_argument("source_service_name", help="Informe o service name do banco de dados oracle de origem")
parser.add_argument("source_owner", help="Informe o nome do owner da tabela de origem")
parser.add_argument("source_table", help="Informe o nome da tabela de origem")
parser.add_argument("target_host_kafka", help="Informe o nome ou IP do kafka de destino, separado por virgula caso tenha mais de um")
parser.add_argument("intervalo_execucao", nargs='?', help="Intervalo de execução em segundos para pegar as mudanças na origem (inteiro), default 300 (5min)", default=300)
parser.add_argument("consumer_timeout", nargs='?', help="Intervalo de tempo em milisegundos que o programa deve esperar por mensagens vindas do topico de controle (inteiro), default 30000 ( 30seg ) ", default=30000)
parser.add_argument("log_level", nargs='?', help="Qual nivel de log deseja para esta execucao [debug,info,warning,error,critical], default info ", default='info')
args = parser.parse_args()


source = args.source
source_host = args.source_host
source_port = args.source_port
source_service_name = args.source_service_name
source_owner = args.source_owner
source_table = args.source_table
target_host_kafka = args.target_host_kafka
target_topic_control = '{}-{}-{}'.format('CONTROLE',source,source_table)
target_topic = '{}-{}'.format(source,source_table)
intervalo_execucao = args.intervalo_execucao
consumer_timeout = args.consumer_timeout
log_level = args.log_level


level = LEVELS.get(log_level, logging.NOTSET)
logging.basicConfig(filename=str(datetime.now().strftime('%Y%m%d_%H%M%S'))+'_cdc.log',level=level)


logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Aplicativo inicializado')
logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' > Log Level: '+log_level)
logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' > Intervalo de execucao: '+str(intervalo_execucao))
logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' > Consumer Timeout: '+str(consumer_timeout))


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
        
        if error_count == error_max:
            logging.error(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Error limit exceeded')
            print(">>> ERROR: Limite de erros excedido!")
            exit(9)
        
        task = MyProcessingThread()
        task.start()
        task.join()

        print('>>> INFO: Aguardando',str(intervalo_execucao),'para próxima execução\n')
        logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' Aguardando '+str(intervalo_execucao)+' segundos para próxima execução')
        sleep(intervalo_execucao)

    except Exception as ex:
        logging.error(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' ' + format_exc() )
        print(">>> ERROR: %s [] %s" % (ex,format_exc()))
        sleep(60)
        error_count += 1

    except KeyboardInterrupt as it:
        logging.info(str(datetime.now().strftime('%d/%m/%Y %H:%M:%S')) + ' CTRL+C acionado')
        print("\n>>> INFO: Ctrl+C acionado, encerrando...")
        exit(2)
