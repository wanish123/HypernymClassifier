import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by talwanich on 25/07/2016.
 */
public class NPFeatureIndex implements Writable, WritableComparable<NPFeatureIndex> {
    public NPFeatureIndex(NounPair pair, int index) {

    }

    @Override
    public int compareTo(NPFeatureIndex o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
