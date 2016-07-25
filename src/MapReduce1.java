import java.io.*;
import java.util.*;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduce1 {




    public static class DPMapper extends Mapper<Object, Text, Text,  NounPair>{


        private HashSet<NounPair> hypernymNounPairs = new HashSet<NounPair>();
        private static final String s3BucketName = "gw-storage-30293052";
        private static final String annotatedSetFileName = "annotated_set.txt";

        @Override
        public void setup(Context context){

            AmazonS3 s3 = new AmazonS3Client();
            S3Object object = s3.getObject(new GetObjectRequest(s3BucketName, annotatedSetFileName));
            BufferedReader br = null;

            try {

                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));

                String sCurrentLine;

                while ((sCurrentLine = br.readLine()) != null) {
                    String[] parts = sCurrentLine.split("\\t");
                    if(parts[2].equals("True")) {
                        NounPair nounPair = new NounPair(parts[0], parts[1]);
                        hypernymNounPairs.add(nounPair);
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (br != null)br.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }

        private List<NounPair> extractNounPairs(Text value) {
            return null;
        }


    }

    public static class FeaturesReducer extends Reducer<Text, NounPair,LongWritable, Text> {

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

        }
    }


    /**
     *
     *
     * args[0] is the path for the corpus, args[1] is the path for output directory
     */
    public static void main(String[] args) throws Exception{

        if(args.length != 2)
            throw new IllegalArgumentException("Usage: " + MapReduce1.class.getSimpleName() + " < inputPath, outputPath , pmiCounters>");

        final Path CORPUS = new Path(args[0]);
        final Path OUTPUT_FEATURES = new Path(args[1]);

        /* STEP 1 */
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "MapReduce1");
        job1.setJarByClass(MapReduce1.class);
        job1.setMapperClass(DPMapper.class);
        job1.setReducerClass(FeaturesReducer.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, CORPUS);
        FileOutputFormat.setOutputPath(job1, OUTPUT_FEATURES);
        job1.waitForCompletion(true);

        /* END - STEP 1*/

    }



}