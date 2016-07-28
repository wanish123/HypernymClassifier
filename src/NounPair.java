import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class NounPair implements Writable, WritableComparable<NounPair> {

    private Text first = new Text();
    private Text second = new Text();
    private Text type = new Text();



    public NounPair(){}


    public NounPair(String first, String second) {
        this.first.set(first);
        this.second.set(second);
    }

    public void setType(MapReduce2.Type type){
        this.type.set(type.toString());
    }

    @Override
    public int compareTo(NounPair o) {
       if(type.equals(MapReduce2.Type.True))
           return 1;
        if(o.type.equals(MapReduce2.Type.True))
            return -1;
        if(type.equals(MapReduce2.Type.False))
            return 1;
        if(o.type.equals(MapReduce2.Type.False))
            return -1;

        int res;
        if((res = first.compareTo(o.first)) != 0)
            return res;

        else return second.compareTo(o.second);

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
        this.first.set(pair.first.toString());
        this.second.set(pair.second.toString());
        this.type = pair.type;
    }
}
