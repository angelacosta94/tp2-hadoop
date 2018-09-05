package bigdata.tp2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;


/**
 * @author Angel Acosta
 */
public class Job1 {
    //Clase encargada del Map
    public static class ClaseMapper extends MapReduceBase implements
            Mapper<
                    LongWritable,/*Input key Type */
                    Text, /*Input value Type*/
                    Text, /*Output key Type*/
                    IntWritable /*Output value Type*/
                    > {
        //Función Map
        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {

            String linea = value.toString();

            String[] campos = linea.split(";");

            // 0      1   2     3
            //Empresa;Año;Mes;Consumo
            String empresa = campos[0];
            String anho = campos[1];
            String mes = campos[2];
            int consumo = Integer.parseInt(campos[3]);

            output.collect(new Text(mes), new IntWritable(consumo));
        }
    }

    //Clase encargada del Reduce
    public static class ClaseReducer extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {

        //Función Reduce
        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

            int consumoTotalAnho = 0;

            while (values.hasNext()) {
                IntWritable next = values.next();
                int consumo = next.get();
                consumoTotalAnho += consumo;
            }
            output.collect(key, new IntWritable(consumoTotalAnho));
        }
    }
}
