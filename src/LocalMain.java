
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by talwanich on 25/07/2016.
 */
public class LocalMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();

        //final String ass3PathName = "s3://gw-storage-30293052/HypernymClassifier";
       // final String corpusFileName = "corpus_debug.txt";
        final String outputFileName = "Output/";

        final Path CORPUS = new Path("input/corpus_debug.txt");
        final Path OUTPUT_FEATURES = new Path(outputFileName);

        /* STEP 1 */
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "MapReduce1");
        job1.setJarByClass(MapReduce1.class);
        job1.setMapperClass(MapReduce1.DPMapper.class);
        job1.setReducerClass(MapReduce1.FeaturesReducer.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, CORPUS);
        FileOutputFormat.setOutputPath(job1, OUTPUT_FEATURES);
        job1.waitForCompletion(true);
        /* END - STEP 1*/

        System.exit(0);

    }
}
