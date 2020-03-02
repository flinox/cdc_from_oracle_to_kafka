#!/home/oracle/local/bin/python
"""
------------------------------------------------------------------------
Author:         Steve Howard
Date:           December 10, 2010
Purpose:        Simple program to process redo log changes
------------------------------------------------------------------------
"""
 
import cx_Oracle, sys, string, _thread, datetime
from threading import Lock
from threading import Thread
from singer import utils
import os

srcuser = None
srcpass = None

plock = _thread.allocate_lock()
 
startYear = 2020
startMonth = 3
startDay = 2
startHour = 7
startMinute = 0
startTime=datetime.datetime(startYear, startMonth, startDay, startHour,startMinute, 0)

endYear = 2020
endMonth = 3
endDay = 2
endHour = 23
endMinute = 59
endTime=datetime.datetime(endYear, endMonth, endDay, endHour, endMinute,0)
 
#-----------------------------------------------------------------------
 
class readRedoThread(Thread):
  def __init__ (self,threadNum):
    Thread.__init__(self)
    self.t = threadNum
 
  def run(self):

    global srcuser,srcpass

    if 'SRCUSER' not in os.environ or 'SRCPASS' not in os.environ:
        print(">>> ERROR: [valida_usuario_senha] Enviroment variables not set, exiting now... ")
        exit(5)

        if None in (os.environ['SRCUSER'],os.environ['SRCPASS']):
            print('>>> ERROR: [valida_usuario_senha] User name and password not set, exiting now...')
            exit(5)

    srcuser = os.environ['SRCUSER'] 
    srcpass = os.environ['SRCPASS'] 

    dsn = cx_Oracle.makedsn('10.63.38.247', 32771,service_name='ORCLCDB.localdomain')

    conn = cx_Oracle.connect(srcuser, srcpass, dsn , encoding="UTF-8",mode=cx_Oracle.SYSDBA)

    cursor = conn.cursor()
    contents = conn.cursor()
 
    # cursor.prepare("select name \
    #                   from v$archived_log \
    #                   where first_time between :1 and :2 + 60/1440 \
    #                     and thread# = :3 \
    #                     and deleted = 'NO' \
    #                     and name is not null \
    #                     and dest_id = 1")
 
    #...and loop until we are past the ending time in which we are interested...
    global startTime
    global endTime
 

    try:
        print('Log Start...')    
        logStart = conn.cursor()

        #you may have to use an "offline" catalog if this is being run 
        #  against a standby database, or against a read-only database.
        #logStart.execute("begin sys.dbms_logmnr.start_logmnr(dictfilename => :1); end;",["/tmp/dictionary.ora"])
        #logStart.execute("begin sys.dbms_logmnr.start_logmnr(options => dbms_logmnr.dict_from_online_catalog \
                                                                        # dbms_logmnr.print_pretty_sql \
                                                                        # dbms_logmnr.no_rowid_in_stmt); end;")

        try:
            logStart.execute("BEGIN DBMS_LOGMNR.START_LOGMNR(startTime => TO_DATE('02/03/2020 18:40:00', 'DD/MM/YYYY HH24:MI:SS'),endTime => TO_DATE('02/03/2020 18:50:00', 'DD/MM/YYYY HH24:MI:SS'),OPTIONS => DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.CONTINUOUS_MINE + DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;")

            print('Rodando...')
            contents.execute("SELECT OPERATION_CODE,OPERATION,COMMIT_TIMESTAMP,SEG_TYPE_NAME,SEG_OWNER,TABLE_NAME,TABLE_SPACE,USERNAME,ROW_ID,SQL_REDO,SQL_UNDO FROM V$LOGMNR_CONTENTS WHERE TABLE_NAME = 'PRODUCAO' AND SEG_OWNER = 'ALUNCURS' AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE') ORDER BY SCN")

            for change in contents:
                plock.acquire()
                print("SQL redo:", change[0])
                print("SCN:", change[0])
                print("sql redo:", change[1])
                plock.release()


            print('Terminando...')

                # registros = cursor.execute(consulta)

                # json_data=[]

                # try:

                #     colunas = [linha[0] for linha in cursor.description]
                #     #registros = cursor.fetchall()

                #     for registro in registros:
                #         json_data.append(dict(zip(colunas,registro)))

                # except Exception as ex:
                #     print(">>> ERROR: [consulta] %s [] %s" % (ex,traceback.format_exc()))
                #     exit(3)

                # print(json.dumps(json_data, default = json_converter, indent=4))

        except Exception as ex:

            print(">>> ERROR: [main] %s [] %s" % (ex,traceback.format_exc()))
            exit(6)


    except cx_Oracle.DatabaseError as ex:
        pass

#-----------------------------------------------------------------------
 
# def restoreLogs():
#  #placeholder for future procedure to get any necessary archived redo
# logs from RMAN
#  pass
 
#-----------------------------------------------------------------------

def main():
      
    threadList = []
    threadNums = []
    global startTime
    global endTime
    #startTime = utils.strptime(config["start_date"])
    #endTime = datetime.datetime.now()
    #print(startTime)

    global srcuser,srcpass

    if 'SRCUSER' not in os.environ or 'SRCPASS' not in os.environ:
        print(">>> ERROR: [valida_usuario_senha] Enviroment variables not set, exiting now... ")
        exit(5)

        if None in (os.environ['SRCUSER'],os.environ['SRCPASS']):
            print('>>> ERROR: [valida_usuario_senha] User name and password not set, exiting now...')
            exit(5)

    srcuser = os.environ['SRCUSER'] 
    srcpass = os.environ['SRCPASS'] 

    dsn = cx_Oracle.makedsn('10.63.38.247', 32771,service_name='ORCLCDB.localdomain')

    conn = cx_Oracle.connect(srcuser, srcpass, dsn , encoding="UTF-8",mode=cx_Oracle.SYSDBA)

    threadNums.append(1)

    print('Iniciando...')

    for i in threadNums:
        thisOne = readRedoThread(i)
        threadList.append(thisOne)
        thisOne.start()

    for j in threadList:
        j.join()


main()