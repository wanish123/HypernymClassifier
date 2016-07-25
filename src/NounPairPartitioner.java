import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by talwanich on 25/07/2016.
 */
public class NounPairPartitioner extends Partitioner {
    @Override
    public int getPartition(Object o, Object o2, int i) {
        return 0;
    }
}
