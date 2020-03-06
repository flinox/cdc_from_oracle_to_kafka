
# A simple CDC_FROM_ORACLE_TO_KAFKA
A Python program to capture data change from table on oracle and send it to kafka topic.


## Version History
```
v0 - 28/01/2020 - By: Fernando Lino e Fernanda Titato
v1 - 06/03/2020 - By: Fernando Lino e Fernanda Titato
```



## Oracle Database Source Pre-Reqs
#### It must be enable at least the Minimal Supplemental Logging.

```
Minimal Supplemental Logging

Registra a quantidade mínima de informações necessárias para o LogMiner identificar, agrupar e mesclar as operações de redo associadas às alterações do DML. Ele garante que o LogMiner (e qualquer produto desenvolvido com a tecnologia LogMiner) tenha informações suficientes para suportar linhas encadeadas e várias disposições de armazenamento, como tabelas de cluster e tabelas organizadas por índices.
```

To check it, you can run the following query:

```
SELECT SUPPLEMENTAL_LOG_DATA_MIN,
       SUPPLEMENTAL_LOG_DATA_PK,
       SUPPLEMENTAL_LOG_DATA_UI
FROM V$DATABASE;
```
 
To enable the Minimal Supplemental Logging, run the following command as Sysdba:
```
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

In order to recover the changes, what we could see is that the user who will perform the queries needs to have permission of SYSDBA.

So, the following script will help to enable the minimal supplemental log and give the write permissions to user created to retrieve the changes from oracle table, remember to replace <user> to your source user with write permissions to access the table who you want to get the data change.

```
alter database archivelog;

ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

grant create session, execute_catalog_role, select any transaction, select any dictionary to <user>;

grant select on SYSTEM.LOGMNR_COL$ to <user>;

grant select on SYSTEM.LOGMNR_OBJ$ to <user>;

grant select on SYSTEM.LOGMNR_USER$ to <user>;

grant select on SYSTEM.LOGMNR_UID$ to <user>;

grant LOGMINING to <user>;

grant EXECUTE_CATALOG_ROLE to <user>;

grant SELECT_CATALOG_ROLE to <user;

grant select on v_$database to <user>;

grant execute on DBMS_LOGMNR to <user>;

grant select on v_$logmnr_contents to <user>;

grant sysdba to <user>;
```

I recommend the use of an Oracle container for tests
```
https://hub.docker.com/_/oracle-database-enterprise-edition
```

The default user for this image from Oracle is:
```
export SRCUSER="sys"
export SRCPASS="Oradoc_dbl"
```


## The Python CDC program requirements:

> Python 3.6+

### libraries
```
pip3 install cx_Oracle
pip3 install singer-python==5.3.1
pip3 install strict-rfc3339==0.7
pip3 install kafka-python
```


### Before run
Set the sysdba user and password of who will have access to table you want to get the change data, like this:

```
export SRCUSER="usuario"
export SRCPASS="senha"
```

### Run
```
python cdc.py SISTEMA 10.63.38.247 1521 ORCLCDB.localdomain OWNER TABLE localhost:9092
```

to get more information


```
python cdc.py -h

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

positional arguments:
  source               Nome de identificacao do sistema de origem, será usado
                       para organizar as mensagens no nome do topico no kafka
  source_host          Informe o nome ou IP do banco de dados oracle de origem
  source_port          Informe a porta do oracle do banco de dados oracle de
                       origem
  source_service_name  Informe o service name do banco de dados oracle de
                       origem
  source_owner         Informe o nome do owner da tabela de origem
  source_table         Informe o nome da tabela de origem
  target_host_kafka    Informe o nome ou IP do kafka de destino, separado por
                       virgula caso tenha mais de um
  intervalo_execucao   Intervalo de execução em segundos para pegar as
                       mudanças na origem (inteiro), default 300 (5min)
  consumer_timeout     Intervalo de tempo em milisegundos que o programa deve
                       esperar por mensagens vindas do topico de controle
                       (inteiro), default 30000 ( 30seg )
  log_level            Qual nivel de log deseja para esta execucao
                       [debug,info,warning,error,critical], default info

optional arguments:
  -h, --help           show this help message and exit
```

## How it works

A control topic will be created to know the date ranges already processed, the content will something like this:
```
{ "START_TIME": "03/03/2020 12:00:00" , "END_TIME": "03/03/2020 12:10:00" }
```
The application will always keep in this control topic the date range used to get the changes in the source table.

In the next run, the application will use END TIME as START TIME and END TIME will be the date and time of the runtime, then it always sending DELTA to the main topic.

## Knownledge Base

If you trying to connect and get the error below:
```
>>> Connecting with the oracle database source ...
python3.6: Relink `/lib/x86_64-linux-gnu/libsystemd.so.0' with `/lib/x86_64-linux-gnu/librt.so.1' for IFUNC symbol `clock_gettime'
python3.6: Relink `/lib/x86_64-linux-gnu/libudev.so.1' with `/lib/x86_64-linux-gnu/librt.so.1' for IFUNC symbol `clock_gettime'
```

Try:
```
sudo apt-get install libaio1
```

## References

https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
https://pypi.org/project/kafka-python/
https://www.programcreek.com/python/example/74085/cx_Oracle.SYSDBA
https://stackoverflow.com/questions/18267935/return-variable-from-cx-oracle-pl-sql-call-in-python
https://cx-oracle.readthedocs.io/en/latest/user_guide/connection_handling.html
https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1



