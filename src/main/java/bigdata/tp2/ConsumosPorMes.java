/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bigdata.tp2;

/**
 *
 * @author Angel Acosta
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class ConsumosPorMes {
    //Clase encargada del Map
    public static class ClaseMapper extends MapReduceBase implements
         Mapper<
            LongWritable,/*Input key Type */
            Text, /*Input value Type*/ 
            Text, /*Output key Type*/ 
            IntWritable /*Output value Type*/
        >
    {
        //Función Map
        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<Text, IntWritable> output,
                Reporter reporter) throws IOException {

            String linea = value.toString();

            String [] campos = linea.split(";");

            // 0      1   2     3
            //Empresa;Año;Mes;Consumo
            String empresa = campos[0];
            String anho = campos[1];
            String mes  = campos[2];
            int consumo = Integer.parseInt(campos[3]);

            output.collect(new Text(mes), new IntWritable(consumo));
        }
    }

    //Clase encargada del Reduce
    public static class ClaseReducer extends MapReduceBase implements
        Reducer< Text, IntWritable, Text, IntWritable> {

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


    public static class MesConsumo implements Writable {
        String mes;
        int consumo = 0;

        public MesConsumo() {}

        public MesConsumo(String mes, int consumo) {
            this.mes = mes;
            this.consumo = consumo;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            mes = WritableUtils.readString(in);
            consumo = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeString(out, mes);
            out.writeInt(consumo);
        }

        @Override
        public String toString() {
            return this.mes + "\t" + this.consumo;
        }

        public String getMes() {
            return mes;
        }

        public void setMes(String mes) {
            this.mes = mes;
        }

        public int getConsumo() {
            return consumo;
        }

        public void setConsumo(int consumo) {
            this.consumo = consumo;
        }
    }

    //Main function 
    public static void main(String args[]) throws Exception {
        JobConf conf = new JobConf(ConsumosPorMes.class);

        conf.setJobName("mes_consumo_maximo");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(ClaseMapper.class);
        //conf.setCombinerClass(ClaseReducer.class);
        conf.setReducerClass(ClaseReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

    }
}
