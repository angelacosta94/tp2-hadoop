#Instala las dependencias, compila el proyecto y crea el jar
mvn clean install package

#Inicia los servicios de hadoop
start-dfs.sh
start-yarn.sh

#Copia el archivo con los datos al hdfs
hdfs dfs -copyFromLocal datos/datos.txt /datos

#Ejecuta el programa
hadoop jar target/tp2-1.0-SNAPSHOT.jar  /datos /salida

#Muestra la salida del programa
hdfs dfs -cat /salida/*

#Borra los datos y la salida del hdfs
hdfs dfs -rm /datos
hdfs dfs -rm -r /salida
