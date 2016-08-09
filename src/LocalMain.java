
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by talwanich on 25/07/2016.
 */
public class LocalMain {

    //private static final String S3_HYPERNYM_PREFIX = "s3://gw-storage-30293052/HypernymClassifier/";
    private final static String CORPUS_BIG = "input/biarcs.02-of-99.txt";
    private final static String CORPUS_SMALL = "input/biarcs.small.txt";
    //private static final String CORPUS = S3_HYPERNYM_PREFIX + CORPUS_SMALL;
    private static final String S3_HYPERNYM_OUTPUT1 = "Output1/";
    private static final String S3_HYPERNYM_OUTPUT2 = "Output2/";
    final static    int DPMIN = 50;
    static final int NUM_OF_REDUCERS = 10;


    final static Path CORPUS_BIG_PATH = new Path(CORPUS_BIG);
    final static Path CORPUS_SMALL_PATH = new Path(CORPUS_SMALL);
    final static Path CORPUS_PATH = CORPUS_SMALL_PATH;

    final static Path S3_HYPERNYM_OUTPUT1_PATH = new Path(S3_HYPERNYM_OUTPUT1);
    final static Path S3_HYPERNYM_OUTPUT2_PATH = new Path(S3_HYPERNYM_OUTPUT2);


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        /* STEP 1 */
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "MapReduce1");
        job1.setJarByClass(MapReduce1.class);
        job1.setMapperClass(MapReduce1.DPMapper.class);
        job1.setMapOutputKeyClass(DependencyPath.class);
        job1.setMapOutputValueClass(NounPair.class);
        job1.setReducerClass(MapReduce1.FeaturesReducer.class);
        job1.setOutputKeyClass(DependencyPath.class);
        job1.setOutputValueClass(DependencyPath.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, CORPUS_PATH);
        FileOutputFormat.setOutputPath(job1, S3_HYPERNYM_OUTPUT1_PATH);

        Configuration conf = job1.getConfiguration();
        conf.setLong("DPMIN", DPMIN);


        job1.waitForCompletion(true);
        /* END - STEP 1*/



        /* STEP 2 */
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "MapReduce2");
        job2.setJarByClass(MapReduce2.class);
        job2.setMapperClass(MapReduce2.FeatureBuilderMapper.class);
        job2.setMapOutputKeyClass(NPFeatureCoordinate.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setReducerClass(MapReduce2.FeaturesVectorBuilderReducer.class);
        job2.setCombinerClass(MapReduce2.FeaturesVectorBuilderReducer.class);
        job2.setOutputKeyClass(NPFeatureCoordinate.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setNumReduceTasks(NUM_OF_REDUCERS);
        job2.setPartitionerClass(NounPairPartitioner.class);

        FileInputFormat.addInputPath(job2, CORPUS_PATH);
        FileOutputFormat.setOutputPath(job2, S3_HYPERNYM_OUTPUT2_PATH);
        job2.waitForCompletion(true);
        /* END - STEP 2*/
        System.exit(0);

    }
}
