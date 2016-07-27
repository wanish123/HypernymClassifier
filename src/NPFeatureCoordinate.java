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

    private NounPair pair;
    private int coordinate;


    public NPFeatureCoordinate(){}

    public NPFeatureCoordinate(NounPair pair, int coordinate) {
        this.pair = pair;
        this.coordinate = coordinate;
    }

   @Override
    public int compareTo(NPFeatureCoordinate o) {
        if(!pair.equals(o.pair))
            return pair.compareTo(o.pair);
        else
            return coordinate - o.coordinate;

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        pair.write(dataOutput);
        LongWritable coordinate = new LongWritable(this.coordinate);
        coordinate.write(dataOutput);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pair.readFields(dataInput);
        LongWritable coordinate = new LongWritable(this.coordinate);
        coordinate.readFields(dataInput);

    }


    public String toString(){
        return pair.toString() + " " + coordinate;
    }

    public NounPair getNounPair() {
        return pair;
    }
}
