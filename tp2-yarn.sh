#Borra los datos y la salida del hdfs para que no cree conflicto si ya existe
./eliminarArchivos.sh

#Instala las dependencias, compila el proyecto y crea el jar
mvn clean install package

#Copia el archivo con los datos al hdfs
hdfs dfs -copyFromLocal datos/datos.csv /datos

#Ejecuta el programa
yarn jar target/tp2-1.0-SNAPSHOT.jar  /datos /salida

#Muestra la salida del programa
./mostrarSalida.sh
