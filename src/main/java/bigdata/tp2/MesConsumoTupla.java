package bigdata.tp2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * @author Angel Acosta
 */
public class MesConsumoTupla implements Writable {
    private String mes;
    private int consumo = 0;

    public MesConsumoTupla(){}

    public MesConsumoTupla(String mes, int consumo) {
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
