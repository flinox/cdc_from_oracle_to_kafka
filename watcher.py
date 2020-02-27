########################################################################
## The Watcher !                                                      ##
## To capture data change from oracle and send to kafka topic         ##
########################################################################
## v1 - 28/01/2020 - Fernando Lino / Fernanda Titato                  ##
##                                                                    ##
## requirements:                                                      ##
## pip3 install cx_Oracle                                             ##
##                                                                    ##
## e.g.:                                                              ##
## python watcher.py 172.20.1.14:32769/ORCLCDB.localdomain 60 OLIMPO  ##
##                                                                    ##
########################################################################

# -*- coding: utf-8 -*-

'''
 export SRCUSER="sys"
 export SRCPASS="Oradoc_dbl"

 export SRCUSER="PRODUCAO"
 export SRCPASS="producao"
'''

import cx_Oracle
import requests
import time
import sys
import xml.etree.ElementTree as ET
import datetime
import argparse
import textwrap
import os

# Realiza conexao com banco
def conexao(cs):
    # Realiza a conexao
    print(">>> Connecting with the oracle database source ...")
    print(cs)
    con = cx_Oracle.connect(cs,mode=cx_Oracle.SYSDBA)
    #con = cx_Oracle.connect(cs)
    print(con.encoding)
    return con

# Conta qtde de registros por origem
def get_change_data_capture(con):
    lista = []
    try:

        # alter session set nls_date_format = 'DD/MM/YYYY HH24:MI:SS';
        registros = """ DECLARE
                        v_count             NUMBER := 0;
                        v_startTime         DATE := TO_DATE('28/01/2020 12:20:42', 'DD/MM/YYYY HH24:MI:SS');
                        v_endTime           DATE := TO_DATE(SYSDATE, 'DD/MM/YYYY HH24:MI:SS');
                        --
                        p_seg_owner   VARCHAR2(30) := 'PRODUCAO' ;
                        p_table_name  VARCHAR2(30) := 'ALUNCURS';
                        BEGIN

                        DBMS_LOGMNR.START_LOGMNR(startTime => v_startTime,
                                                    endTime => v_endTime,
                                                    OPTIONS => DBMS_LOGMNR.COMMITTED_DATA_ONLY +
                                                    DBMS_LOGMNR.CONTINUOUS_MINE +
                                                    DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG 
                                                );
                                                
                        FOR item IN (SELECT  OPERATION_CODE,
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
                                        WHERE TABLE_NAME = p_table_name
                                        AND SEG_OWNER = p_seg_owner
                                        AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
                                        ORDER BY SCN
                                        )    
                        LOOP                           
                            --
                            v_count := v_count + 1;
                            
                            dbms_output.put_line  (v_count || 'º COMANDO');
                            dbms_output.put_line  ('OPERATION_CODE     : ' || item.OPERATION_CODE);
                            dbms_output.put_line  ('OPERATION          : ' || item.OPERATION);
                            dbms_output.put_line  ('COMMIT_TIMESTAMP   : ' || item.COMMIT_TIMESTAMP);
                            dbms_output.put_line  ('SEG_TYPE_NAME      : ' || item.SEG_TYPE_NAME);
                            dbms_output.put_line  ('SEG_OWNER          : ' || item.SEG_OWNER);
                            dbms_output.put_line  ('TABLE_NAME         : ' || item.TABLE_NAME);
                            dbms_output.put_line  ('TABLE_SPACE        : ' || item.TABLE_SPACE);
                            dbms_output.put_line  ('USERNAME           : ' || item.USERNAME);
                            dbms_output.put_line  ('ROW_ID             : ' || item.ROW_ID);
                            dbms_output.put_line  ('SQL_REDO           : ' || item.SQL_REDO);
                            dbms_output.put_line  ('SQL_UNDO           : ' || item.SQL_UNDO);
                            dbms_output.put_line  ('startTime          : ' || v_startTime);
                            dbms_output.put_line  ('endTime            : ' || v_endTime);
                            
                            dbms_output.put_line (chr(13));
                        
                        END LOOP;
                        DBMS_LOGMNR.END_LOGMNR; 
                        
                        exception
                        when others then
                            dbms_output.put_line  (sqlerrm);

                        END;"""



        registros = """ DECLARE
                        v_temp        VARCHAR2(20);
                        BEGIN

			SELECT  TMACOD INTO v_temp 
                        FROM PRODUCAO.ALUNCURS
                        WHERE ROWNUM < 2;
                        
                        dbms_output.put_line  ('COMANDO!'||v_temp);

                        END;"""

        registros = registros.encode('ascii','ignore').decode('ascii')
        print(registros)

        try:

            cur = con.cursor()
            cur.execute(registros)
            con.commit()

        except cx_Oracle.DatabaseError as e: 
            print("There is a problem with Oracle", e) 
        
        # by writing finally if any error occurs 
        # then also we can close the all database operation 
        finally: 
            if cur: 
                cur.close() 
            if con: 
                con.close() 
        
        time.sleep(1)

        for result in cur:
            #lista.append({result[0].strip(" ") : int(result[1])})
            lista.append(result[0].strip(" "))
            print(lista)
        
        exit(9)

    except Exception as e:

        print('\n################################## ERROR ##################################')
        print("\n\n %s \n %s \n\n" % (sys.exc_info()[0],e))
        print('\n################################## ERROR ##################################\n')


if not sys.version_info > (2, 7):
   # berate your user for running a 10 year
   # python version
   print('>>> This is not work on this python version, you need to upgrade to >= 3.6')
   exit(777)
elif not sys.version_info >= (3, 5):
   # Kindly tell your user (s)he needs to upgrade
   # because you're using 3.5 features
   print('>>> I strongly recommend that you update the python version to >= 3.6')
   exit(555)

# Define os parametros esperados e obrigatorios
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,description=textwrap.dedent('''\
         Watcher - A Change data capture from oracle to kafka topic! 
         ----------------------------------------------------------------------------------------
         Created By: Fernando Lino Di Tomazzo Silva ( https://www.linkedin.com/in/flinox )
         Version 1.0 - 2020-01-28 

         Enviroment Variables:
         SRCUSER >> The user with privileges to get the change data capture from source oracle
         SRCPASS >> The pass of user above

         '''))
parser.add_argument("connectstring", help="The address to database oracle you want to get change data capture, e.g.: hostname:port/service")
parser.add_argument("timeinterval", help="Informe o intervalo de tempo para verificar mudanças em segundos", default=60, type=int)
parser.add_argument("source", help="Informe o nome do sistema de origem")
args = parser.parse_args()

print(">"*80 +"\n")
print(">>> Setting parameters and checking enviroment variables ...")

listaorigens = []
origem = ''
timeinterval = args.timeinterval

if 'SRCUSER' not in os.environ or 'SRCPASS' not in os.environ:
    print("!!! SRCUSER and SRCPASS not set, exiting now... ")
    exit(999)

srcuser = os.environ['SRCUSER'] 
srcpass = os.environ['SRCPASS'] 

connectstring = srcuser+'/'+srcpass+'@'+args.connectstring
#connectstring = srcuser+'/'+srcpass+'@'+args.connectstring+' as sysdba'

print(">>> Starting to get the change data capture %s " % datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

while True:
    try:

        # Realiza a conexão com o banco ...

        con = conexao(connectstring)
        listaorigens = get_change_data_capture(con)

        # for item in listaorigens:
        #     for key,val in item.items():

        #         origem = key
        #         origem_qtde = val

        #         recount = 0
        #         while(origem_qtde > 0):

        #             qtdedeleted = 0

        #             # Deleta tabela auxiliar, insere novos registros selecionados
        #             try:
        #                 # Seleciona 100 registros para expurgo
        #                 print(">>> Selecionando um timeinterval de até %s registros para expurgo da origem %s ..." % ((timeinterval),origem))
        #                 queryselect = "SELECT XRPE.ROWID_XREF FROM CMX_ORS.@mdmbo@_XREF XRPE INNER JOIN CMX_ORS.@mdmbo@ BOPE ON (XRPE.ROWID_OBJECT = BOPE.ROWID_OBJECT) WHERE BOPE.HUB_STATE_IND = -1 AND XRPE.ROWID_SYSTEM = '@origem@' AND ROWNUM < @timeinterval@"
        #                 queryselect = queryselect.replace('@origem@',origem)
        #                 queryselect = queryselect.replace('@timeinterval@',str(timeinterval+1))
        #                 queryselect = queryselect.replace('@mdmbo@',str(mdmbo))

        #                 cur = con.cursor()

        #                 # Deleta a tabela atual
        #                 print(">>> Limpando a tabela CMX_ORS.%s ..." % mdmtabaux)
        #                 querydelete = ("DELETE FROM CMX_ORS.%s" % mdmtabaux)

        #                 cur.execute(querydelete)
        #                 con.commit()
        #                 time.sleep(1)

        #                 print(">>> Inserindo os rowid_xref dos registros selecionados na CMX_ORS.PURGE_AUX ...")
        #                 queryinsert = ("INSERT INTO CMX_ORS.%s %s " % (mdmtabaux,queryselect))

        #                 cur.execute(queryinsert)
        #                 con.commit()
        #                 time.sleep(1)

        #             except:
        #                 e = sys.exc_info()[0]
        #                 print("Error: %s" % e)
        #                 print(sys.exc_info())

        #             finally:

        #                 # Encerra a conexao com o banco
        #                 print(">>> Commitando as transacoes ...")
        #                 time.sleep(1)
        #                 cur.close()
        #                 con.commit()


        #             # Realiza a chamada do serviço para o expurgo
        #             try:

        #                 # Realiza a chamada do serviço para o expurgo
        #                 print(">>> Executando o serviço SIF ExecuteBatchDelete do MDM ...")

        #                 headers = {'content-type': 'text/xml', 'SOAPAction': 'POST'}
        #                 body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:siperian.api">
        #                 <soapenv:Header/>
        #                 <soapenv:Body>
        #                     <urn:executeBatchDelete>
        #                         <!--Optional:-->
        #                         <urn:username>@mdmuser@</urn:username>
        #                         <!--Optional:-->
        #                         <urn:password>
        #                         <urn:password>@mdmpass@</urn:password>
        #                         <urn:encrypted>false</urn:encrypted>
        #                         </urn:password>
        #                         <!--Optional:-->
        #                         <urn:orsId>@mdmorsid@</urn:orsId>
        #                         <!--Optional:-->
        #                         <urn:interactionId></urn:interactionId>
        #                         <!--Optional:-->
        #                         <urn:asynchronousOptions>
        #                         <urn:isAsynchronous>false</urn:isAsynchronous>
        #                         <!--Optional:-->
        #                         <urn:jmsReplyTo></urn:jmsReplyTo>
        #                         <!--Optional:-->
        #                         <urn:jmsCorrelationId></urn:jmsCorrelationId>
        #                         </urn:asynchronousOptions>
        #                         <urn:tableName>@mdmbo@</urn:tableName>
        #                         <urn:sourceTableName>@mdmtabaux@</urn:sourceTableName>
        #                         <urn:recalculateBvt>@recalculatebvt@</urn:recalculateBvt>
        #                         <urn:cascading>@cascading@</urn:cascading>
        #                         <urn:overrideHistory>@overridehistory@</urn:overrideHistory>
        #                         <urn:purgeHistory>@purgehistory@</urn:purgeHistory>
        #                     </urn:executeBatchDelete>
        #                 </soapenv:Body>
        #                 </soapenv:Envelope>"""

        #                 body = body.replace("@mdmuser@",mdmuser)
        #                 body = body.replace("@mdmpass@",mdmpass)
        #                 body = body.replace("@mdmbo@",mdmbo)
        #                 body = body.replace("@mdmtabaux@",mdmtabaux)
        #                 body = body.replace("@mdmorsid@",mdmorsid)
        #                 body = body.replace("@recalculatebvt@",recalculatebvt)
        #                 body = body.replace("@cascading@",cascading)
        #                 body = body.replace("@overridehistory@",overridehistory)
        #                 body = body.replace("@purgehistory@",purgehistory)

        #                 response = requests.post(url,data=body,headers=headers)

        #                 print("")
        #                 print(response.content.decode('utf8'))

        #                 xml=ET.fromstring(response.content.decode('utf8'))
        #                 for child in xml[0][0]:
        #                         if child.tag == "{urn:siperian.api}processedXrefsCount":
        #                             qtdedeleted = int(child.text)

        #                 print("")
        #                 print(">>> Registros deletados: %s " % str(qtdedeleted))
        #                 print("")      

        #             except:
        #                 e = sys.exc_info()[0]
        #                 print("Error: %s" % e)

        #             finally:

        #                 print(">"*80 +"\n")      

                   
        #             if qtdedeleted == 0:
                        
        #                 recount += 1

        #                 if recount > 6:

        #                     recountqry = "SELECT COUNT(1) FROM CMX_ORS.@mdmbo@_XREF XRPE INNER JOIN CMX_ORS.@mdmbo@ BOPE ON (XRPE.ROWID_OBJECT = BOPE.ROWID_OBJECT) WHERE BOPE.HUB_STATE_IND = -1 AND XRPE.ROWID_SYSTEM = '@origem@'"
        #                     recountqry = recountqry.replace('@origem@',origem)
        #                     recountqry = recountqry.replace('@mdmbo@',str(mdmbo))

        #                     cur = con.cursor()
        #                     cur.execute(recountqry)

        #                     for result in cur:
        #                         origem_qtde = int(result[0])

        #                     if origem_qtde == 0:
        #                         print(">>> Não existem mais registros para expurgar da origem %s " % origem)  
        #                         continue
                            
        #                     cur.close()

        #             origem_qtde = origem_qtde - ( qtdedeleted )
        #             print(">>> Processo em execucao %s " % datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))   
        #             print(">>> Restando %s registros inativos pendentes para %s ..." % (origem_qtde,origem))

        #         print(">>> timeinterval finalizado para origem %s " % origem)
        #         print(">>> Termino do timeinterval em %s <<< " % datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

        # Encerrando a conexão com o banco
        con.close()
        print(">>> Finishing %s " % datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        print("")
        print(">"*80 +"\n")
        exit(0)

    except KeyboardInterrupt:

        print(">>> Finishing Process by user request ( CTRL + C ) <<<")
        print(">"*80 +"\n")
        exit(888)

