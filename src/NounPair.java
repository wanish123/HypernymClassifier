import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class NounPair implements Writable, WritableComparable<NounPair> {


    private Word first = new Word();
    private Word second = new Word();
    private Text type = new Text(MapReduce2.Type.Unknown.toString());

    public NounPair(){}

    public NounPair(String first, String POS1, String second, String POS2){
        this.first.set(first, POS1);
        this.second.set(second, POS2);
    }

    public NounPair(String first, String second) {
        this.first.set(first);
        this.second.set(second);

    }

    public NounPair(String first, String second, MapReduce2.Type type) {
        this.first.set(first);
        this.second.set(second);
        this.type.set(type.toString());

    }

    public void setType(MapReduce2.Type type){
        this.type.set(type.toString());
    }

    @Override
    public int compareTo(NounPair o) {

        if(type.toString().equals(o.type.toString())){

            if(!first.equals(o.first))
                return first.compareTo(o.first);

            return second.compareTo(o.second);

        }

        return type.toString().compareTo(o.type.toString());



    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.first.write(dataOutput);
        this.second.write(dataOutput);
        this.type.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first.readFields(dataInput);
        this.second.readFields(dataInput);
        this.type.readFields(dataInput);

    }

    public String toString(){
        return first + " " + second + " " + type;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (!NounPair.class.isAssignableFrom(obj.getClass()))
            return false;

        final NounPair other = (NounPair) obj;

        if(!(this.first.equals(other.first) && (this.second.equals(other.second))))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += this.first.hashCode();
        hash += this.second.hashCode();
        return hash;

    }

    public void set(NounPair pair) {
        this.first.set(pair.first.getWord(), pair.first.getPartOfSpeech());
        this.second.set(pair.second.getWord(), pair.second.getPartOfSpeech());
        this.type.set(pair.type.toString());
    }
}
