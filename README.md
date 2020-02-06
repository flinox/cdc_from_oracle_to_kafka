
# The Watcher !                                      
To capture data change from oracle and send to kafka topic

## Version History
```
v1 - 28/01/2020 - By: Fernando Lino e Fernanda Titato
```

## Requirements:
### Install library oracle python
```
sudo apt install python3-pip
pip3 install cx_Oracle
pip3 install kafka-python
```

### Install oracle container para testes 
```
https://hub.docker.com/_/oracle-database-enterprise-edition
```

## How to use:

### Set the user and pass to access the source
```
export SRCUSER="sys"
export SRCPASS="Oradoc_dbl"
```

### e.g.
```
python watcher.py 172.20.1.23:32769/ORCLCDB 60 ORIGEM
```

```
Watcher - A Change data capture from oracle to kafka topic! 
----------------------------------------------------------------------------------------
Created By: Fernando Lino Di Tomazzo Silva ( https://www.linkedin.com/in/flinox )

Enviroment Variables:
SRCUSER >> The user with privileges to get the change data capture from source oracle
SRCPASS >> The pass of user above

positional arguments:
  connectstring  The address to database oracle you want to get change data
                 capture, e.g.: hostname:port/service
  timeinterval   Informe o intervalo de tempo para verificar mudanças em
                 segundos
  source         Informe o nome do sistema de origem

optional arguments:
  -h, --help     show this help message and exit
```






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