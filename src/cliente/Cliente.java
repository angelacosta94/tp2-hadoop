/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cliente;
import java.net.*;
import java.io.*;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

/**
 *
 * @author IVAN
 */
public class Cliente {

    final String HOST = "localhost";
    final int PUERTO=5000;
    Socket sc;
    DataOutputStream mensaje;
    DataInputStream entrada;
//Cliente
    public void initClient(){ /*ejecuta este metodo para correr el cliente */
        try{
            sc = new Socket( HOST , PUERTO ); /*conectar a un servidor en localhost con puerto 5000*/
//creamos el flujo de datos por el que se enviara un mensaje
            mensaje = new DataOutputStream(sc.getOutputStream());
//enviamos el mensaje
            mensaje.writeUTF("hola que tal!!");
//cerramos la conexión
            sc.close();
        }catch(Exception e ){
           System.out.println("Error: "+e.getMessage());
        }
    }
     public static void main(String[] args) {
       
        Cliente cliente = new Cliente();
        cliente.initClient();
    }
}



