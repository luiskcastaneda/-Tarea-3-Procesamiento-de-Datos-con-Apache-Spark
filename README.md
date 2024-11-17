<h1>Implementación en Spark:</h1>
Se desarrolla una aplicación en Python para procesamiento en batch
<h3>Paso 1:</h3> Se inicia la máquina virtual con las credenciales
Usuario: hadoop 
Password: hadoop
Esto nos generará la IP de acceso a la máquina virtual
<h3>Paso 2:</h3> Nos conectamos por Putty a la maquina virtual mediante la IP generada
<h3>Paso 3:</h3> Ingresamos con las siguientes credenciales 
Usuario: hadoop 
Password: hadoop 
<h3>Paso 4:</h3> Iniciamos Hadoop con el comando start-all.sh
<h3>Paso 5:</h3> Una vez iniciados todos los servicios, se puede acceder al Hadoop de la siguiente manera: tomamos la ip que se genero a l inicio, al final colocamos : y el puerto 9870 para acceder mediante el navegador. http://IP_MaquinaVirtual:9870
<h3>Paso 6:</h3> Una vez tenemos hadoop iniciado procedemos a crear el archivo que necesitamos para ejecutar las operaciones básicas con Spark. Una vez logueados en putty, creamos una carpeta para almacenar nuestro archivo, esto se realiza mediante le comando hdfs dfs -mkdir /Tarea3, con esto creamos una carpeta llamada Tarea3 en el sistema HDFS
<h3>Paso 7:</h3> Ingresamos a https://www.datos.gov.co/browse y buscamos un dataset que cumpla con las condiciones planteadas, ingresamos al Dataset, Luego se da clic en Exportar, Damos clic derecho sobre la opción CSV y luego en copiar vínculo, Abrimos un Bloc de notas y damos Ctrl + V o pegar, eliminamos de la URL ?accessType=DOWNLOAD quedando de la siguiente forma. Esta será la dirección de descarga del dataset, https://www.datos.gov.co/api/views/mcec-87by/rows.csv
<h3>Paso 8:</h3> En la máquina virtual descargamos el dataset con la URL obtenida en el anterior paso wget https://www.datos.gov.co/api/views/mcec-87by/rows.csv
<h3>Paso 9:</h3> Ahora copiamos el archivo del Dataset descargado a la carpeta HDFS que creamos hdfs dfs -put /home/hadoop/rows.csv /Tarea3
<h3>Paso 10:</h3> Se crea un archivo de python con extension .py lo podemos llamar tarea3.py, donde colocaremos el código del programa que se conectará al dataset descargado dentro del sistema HDFS, luego convertiremos estos datos en un dataframe de Spark y sobre este dataframe se realizarán diferentes operaciones y consultas. 

Código para crear el archivo: nano tarea3.py
<h3>Paso 11:</h3> oprimimos la secuencia de teclas Crtl+O enter y luego Crtl+X para salir, esto guardará el archivo y saldar de nuevo a la consola.
<h3>Paso 12:</h3> para ejecutar el archivo ingresamos la siguiente instrucción: 
python3 tarea3.py

Una vez terminada la ejecución en consola se mostrara el resultado con los parámetros que anteriormente se parametrizaron en el archivo.

<h1>Análisis de Datos en Tiempo Real con Spark Streaming y Kafka</h1>

<h3>Paso 1:</h3> ingresamos con putty en la máquina virtual configurada con hadoop y spark con los datos :
Usuario: vboxuser 
Password: bigdata
<h3>Paso 2:</h3> si no tenemos instalada la librería de Python Kafka, la podemos instalar mediante el comando PIP:  pip install kafka-python
<h3>Paso 3:</h3> Descargue, descomprima y mueva de carpeta Apache Kafka  wget https://downloads.apache.org/kafka/3.6.2/kafka_2.13-3.6.2.tgz, y descomprimimos con 
tar -xzf kafka_2.13-3.6.2.tgz
<h3>Paso 4:</h3> movemos los archivos con el comando 
sudo mv kafka_2.13-3.6.2 /opt/Kafka
<h3>Paso 5:</h3> Iniciamos el servidor ZooKeeper con el comando
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
<h3>Paso 6:</h3> Después de un momento y terminada la ejecución del comando anterior se debe dar Enter para que aparezca nuevamente el prompt del sistema
<h3>Paso 7:</h3> Iniciamos el servidor Kafka con el comando 
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
<h3>Paso 8:</h3> Después de un momento y terminada la ejecución del comando anterior se debe dar Enter para que aparezca nuevamente el prompt del sistema
<h3>Paso 9:</h3> Creamos un tema (topic) de Kafka, el tema se llamará sensor_data con el siguiente comando
 /opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 - replication-factor 1 --partitions 1 --topic sensor_data
<h3>Paso 10:</h3> implementamos del productor(producer) de Kafka creando un archivo llamado kafka_producer.py  con el comando 
nano kafka_producer.py
este archivo tendrá el código paramétrico para la ejecución del producer
<h3>Paso 11:</h3> con la combinación de teclas  Crtl+O enter y luego Crtl+X para salir guardamos el archivo y regresamos a la consola
Este script genera datos simulados de sensores y los envía al tema (topic) de Kafka que creamos anteriormente (sensor_data).
<h3>Paso 12:</h3>creamos un consumidor(consumer) utilizando Spark Streaming para procesar los datos en tiempo real, se debe crear un archivo llamado spark_streaming_consumer.py con el siguiente comando
nano spark_streaming_consumer.py
<h3>Paso 13:</h3> con la combinación de teclas  Crtl+O enter y luego Crtl+X para salir guardamos el archivo y regresamos a la consola
Este script utiliza Spark Streaming para leer datos del tema(topic) de Kafka, procesa los datos en ventanas de tiempo de 1 minuto y calcula la temperatura y humedad promedio para cada sensor.
<h3>Paso 14:</h3> 
Ejecución y análisis 
En una terminal, ejecutamos el productor(producer) de Kafka: con el comando
python3 kafka_producer.py 
En otra terminal, ejecutamos el consumidor de Spark Streaming con el comando
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
<h3>Paso 15:</h3> Observamos la salida del productor y del consumidor de Spark Streaming y analizamos los resultados
<h3>Paso 16:</h3> Podemos ver información sobre los Jobs, Stages, entre otros, realizados por Spark, para esto ingresamos a la consola web http://your-server-ip:4040

<h1>NOTA: es importante primero ejecutar el script de productor y luego el del consumidor</h1>
