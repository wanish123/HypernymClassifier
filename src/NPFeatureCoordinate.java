import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by talwanich on 25/07/2016.
 */
public class NPFeatureCoordinate implements Writable, WritableComparable<NPFeatureCoordinate> {

    private NounPair pair = new NounPair();
    private LongWritable coordinate = new LongWritable();


    public NPFeatureCoordinate(){}

    public NPFeatureCoordinate(NounPair pair, int coordinate) {
        this.pair.set(pair);
        this.coordinate.set(coordinate);
    }

   @Override
    public int compareTo(NPFeatureCoordinate o) {
        if(!pair.equals(o.pair))
            return pair.compareTo(o.pair);
        else
            return coordinate.compareTo(o.coordinate);

    }

    @Override
    public int hashCode() {
        return this.pair.hashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        pair.write(dataOutput);
        coordinate.write(dataOutput);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pair.readFields(dataInput);
        coordinate.readFields(dataInput);

    }


    public String toString(){
        return pair.toString() + " " + coordinate;
    }

    public NounPair getNounPair() {
        return pair;
    }
}
