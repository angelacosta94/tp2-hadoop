#Muestra la salida del programa
echo Salida del job 1
hdfs dfs -cat /salida_tmp/*

echo Salida del job 2
hdfs dfs -cat /salida/*

