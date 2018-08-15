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
public class Job2 {
    //Clase encargada del Map
    public static class ClaseMapper extends MapReduceBase implements
            Mapper<
                    LongWritable,/*Input key Type */
                    Text, /*Input value Type*/
                    Text, /*Output key Type*/
                    MesConsumoTupla /*Output value Type*/
                    > {
        //Función Map
        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, MesConsumoTupla> output,
                        Reporter reporter) throws IOException {

            String linea = value.toString();

            String[] campos = linea.split("\t");

            // 0        1
            //Mes \t Consumo
            String mes = campos[0];
            int consumo = Integer.parseInt(campos[1]);

            MesConsumoTupla tupla = new MesConsumoTupla(mes, consumo);

            output.collect(new Text("MAXIMO"), tupla);
        }
    }

    //Clase encargada del Reduce
    public static class ClaseReducer extends MapReduceBase implements
            Reducer<Text, MesConsumoTupla, Text, IntWritable> {

        //Función Reduce
        @Override
        public void reduce(Text key, Iterator<MesConsumoTupla> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

            int consumoMaximo = 0;
            String mesConsumoMaximo = "";

            while (values.hasNext()) {
                MesConsumoTupla next = values.next();
                int consumo = next.getConsumo();

                if (consumo > consumoMaximo) {
                    consumoMaximo = consumo;
                    mesConsumoMaximo = next.getMes();
                }
            }

            output.collect(new Text(mesConsumoMaximo), new IntWritable(consumoMaximo));
        }
    }
}
