package bigdata.tp2;

import java.io.*;
import java.util.Random;

public class GeneradorDeDatos {
    private static final int NUM_EMPRESAS = 300000;
    private static final int ANHO_INICIO = 1980;
    private static final int ANHO_FIN = 2015;

    public static void main(String args[]){
        try
        {
            File archivo = new File("datos/datos.csv");
            PrintWriter pw = new PrintWriter(archivo);

            String [] meses = {"ENE", "FEB", "MAR", "ABR", "MAY", "JUN", "JUL", "AGO", "SEP", "OCT", "NOV", "DIC"};

            Random rand = new Random();

            for (int empresa = 1; empresa <= NUM_EMPRESAS; empresa++  ){

                for(int anho = ANHO_INICIO; anho <= ANHO_FIN; anho ++){

                    for (int mes = 0; mes < 12; mes++){
                        int consumo = 30 + mes % 10 + rand.nextInt(100);
                        pw.println(empresa + ";" + anho + ";" + meses[mes] + ";" + consumo);
                    }
                    pw.flush();

                }

            }

            pw.close();

        }  catch (FileNotFoundException fnf){
            System.out.println("No se pudo encontrar el archivo");
        }
    }


}
