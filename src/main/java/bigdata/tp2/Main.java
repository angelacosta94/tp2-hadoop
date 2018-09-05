package bigdata.tp2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


/**
 * @author Angel Acosta
 */
public class Main {
    //Main function
    public static void main(String args[]) throws Exception {
        JobConf confJob1 = new JobConf(Job1.class);

        confJob1.setJobName("mes_consumo_maximo_job_1");

        //Especifica las clases mapper y reducer
        confJob1.setMapperClass(Job1.ClaseMapper.class);
        confJob1.setReducerClass(Job1.ClaseReducer.class);

        //Configuracion del mapper
        confJob1.setMapOutputKeyClass(Text.class);
        confJob1.setMapOutputValueClass(IntWritable.class);

        //Configuracion del reducer
        confJob1.setOutputKeyClass(Text.class);
        confJob1.setOutputValueClass(IntWritable.class);

        //Configuracion del formato de entrada y salida
        confJob1.setInputFormat(TextInputFormat.class);
        confJob1.setOutputFormat(TextOutputFormat.class);

        //Configuracion del directorio de entrada y salida
        FileInputFormat.setInputPaths(confJob1, new Path(args[0]));
        FileOutputFormat.setOutputPath(confJob1, new Path(args[1] + "_tmp"));

        //Ejecuta el job1
        RunningJob job1 = JobClient.runJob(confJob1);

        job1.waitForCompletion();

        boolean jobSuccessful = job1.isSuccessful();

        if (jobSuccessful) {
            JobConf confJob2 = new JobConf(Job2.class);

            confJob2.setJobName("mes_consumo_maximo_job_2");

            //Especifica las clases mapper y reducer
            confJob2.setMapperClass(Job2.ClaseMapper.class);
            confJob2.setReducerClass(Job2.ClaseReducer.class);

            //Configuracion del mapper
            confJob2.setMapOutputKeyClass(Text.class);
            confJob2.setMapOutputValueClass(MesConsumoTupla.class);

            //Configuracion del reducer
            confJob2.setOutputKeyClass(Text.class);
            confJob2.setOutputValueClass(IntWritable.class);

            //Configuracion del formato de entrada y salida
            confJob2.setInputFormat(TextInputFormat.class);
            confJob2.setOutputFormat(TextOutputFormat.class);

            //Configuracion del directorio de entrada y salida
            FileInputFormat.setInputPaths(confJob2, new Path(args[1] + "_tmp"));
            FileOutputFormat.setOutputPath(confJob2, new Path(args[1]));

            //Ejecuta el job2
            RunningJob job2 = JobClient.runJob(confJob2);

            job2.waitForCompletion();

        }
    }
}
