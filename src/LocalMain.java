
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
    //AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();

    //final String ass3PathName = "s3://gw-storage-30293052/HypernymClassifier";
    // final String corpusFileName = "corpus_debug.txt";
    final static  String outputFileNameStep1 = "OutputStep1/";
    final static String outputFileNameStep2 = "OutputStep2/";
    final static    int DPMIN = 1;

    final static Path CORPUS = new Path("input/corpus_debug.txt");
    final static Path OUTPUT_FEATURES = new Path(outputFileNameStep1);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        /* STEP 1 */
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "MapReduce1");
        job1.setJarByClass(MapReduce1.class);
        job1.setMapperClass(MapReduce1.DPMapper.class);
        job1.setReducerClass(MapReduce1.FeaturesReducer.class);
        job1.setOutputKeyClass(DependencyPath.class);
        job1.setOutputValueClass(NounPair.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, CORPUS);
        FileOutputFormat.setOutputPath(job1, OUTPUT_FEATURES);

        Configuration conf = job1.getConfiguration();
        conf.setLong("DPMIN", DPMIN);


        job1.waitForCompletion(true);
        /* END - STEP 1*/

        final int NUM_OF_REDUCERS = 3;
        final Path FeaturesVector_Output = new Path(outputFileNameStep2);


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

        FileInputFormat.addInputPath(job2, OUTPUT_FEATURES);
        FileOutputFormat.setOutputPath(job2, FeaturesVector_Output);
        job2.waitForCompletion(true);
        /* END - STEP 2*/
        System.exit(0);

    }
}
