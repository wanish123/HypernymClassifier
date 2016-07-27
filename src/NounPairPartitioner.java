import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by talwanich on 25/07/2016.
 */
public class NounPairPartitioner extends Partitioner<NPFeatureCoordinate, LongWritable>{

    @Override
    public int getPartition(NPFeatureCoordinate npFeatureCoordinate, LongWritable longWritable, int numOfReducers) {
        int hashCode = Math.abs(npFeatureCoordinate.getNounPair().hashCode());
        return hashCode % numOfReducers;
    }
}
