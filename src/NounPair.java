import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by talwanich on 25/07/2016.
 */
public class NounPair implements Writable, WritableComparable<NounPair> {

    private Text first = new Text();
    private Text second = new Text();

    public NounPair(){}

    public NounPair(String first, String second) {
        this.first.set(first);
        this.second.set(second);

    }

    @Override
    public int compareTo(NounPair o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.first.write(dataOutput);
        this.second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first.readFields(dataInput);
        this.second.readFields(dataInput);
    }

    public String toString(){
        return first + " " + second;
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
}
