import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by talwanich and gilshelef on 08/06/2016.
 */
public class MapReduce2 {

    public static class FeatureBuilderMapper extends Mapper<Object, Text, NPFeatureIndex, LongWritable> {

        private HashSet<NounPair> hypernymNounPairs = new HashSet<NounPair>();
        private HashSet<NounPair> nonHypernymNounPairs = new HashSet<NounPair>();
        private static List<DependencyPath> features = new LinkedList<DependencyPath>();

        private static final String s3BucketName = "gw-storage-30293052";
        private static final String annotatedSetFileName = "annotated_set.txt";
        private static AmazonS3 s3;
        @Override
        public void setup(Context context){

            s3 = new AmazonS3Client();
            initializeHyperSets();
            initializeFeaturesList();

        }
        //TODO
        private void initializeFeaturesList() {
        }

        private void initializeHyperSets() {
            S3Object object = s3.getObject(new GetObjectRequest(s3BucketName, annotatedSetFileName));
            BufferedReader br = null;

            try {

                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));

                String sCurrentLine;

                while ((sCurrentLine = br.readLine()) != null) {
                    String[] parts = sCurrentLine.split("\\t");
                    NounPair nounPair = new NounPair(parts[0], parts[1]);
                    if(parts[2].equals("True"))
                        hypernymNounPairs.add(nounPair);
                    else
                        nonHypernymNounPairs.add(nounPair);
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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

            int index;
            NPFeatureIndex npAndIndex;
            int occurrences = getOccurrences(value);

            value = stem(value);

            List<NounPair> nounPairList = extractNounPairs(value);
            for(NounPair pair : nounPairList) {
                DependencyPath dp = getDependencyPath(pair, value);
                if ((index = features.indexOf(dp)) != -1) {
                    npAndIndex = new NPFeatureIndex(pair, index);
                    context.write(npAndIndex, new LongWritable(occurrences));
                }
            }

        }

        private int getOccurrences(Text value) {
            return 0;
        }


        //TOCOPY
        private DependencyPath getDependencyPath(NounPair pair, Text value) {
            return  null;
        }

        //TOCOPY
        private Text stem(Text value) {
            return  null;
        }

        //TOCOPY
        //Don't forget to find the pair tag
        private List<NounPair> extractNounPairs(Text value) {
            return null;
        }


    }

    public static class FeaturesVectorBuilderReducer extends Reducer<NPFeatureIndex ,LongWritable, NPFeatureIndex, LongWritable> {

        @Override
        public void setup(Context context){
        }
        public void reduce(NPFeatureIndex key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            Iterator<LongWritable> iter = values.iterator();
            long sum = 0;
            while(iter.hasNext())
                sum += iter.next().get();

            context.write(key, new LongWritable(sum));

        }



    }


    public static void main(String[] args) throws Exception {

        if(args.length != 2)
            throw new IllegalArgumentException("Usage: " + MapReduce1.class.getSimpleName() + " < inputPath, outputPath , numOfReducers>");

        final Path CORPUS = new Path(args[0]);
        final Path FeaturesVector_Output = new Path(args[1]);
        final int NUM_OF_REDUCERS = Integer.parseInt(args[2]);

        /* STEP 2 */
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "MapReduce2");
        job2.setJarByClass(MapReduce2.class);
        job2.setMapperClass(FeatureBuilderMapper.class);
        job2.setMapOutputKeyClass(NPFeatureIndex.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setReducerClass(FeaturesVectorBuilderReducer.class);
        job2.setCombinerClass(FeaturesVectorBuilderReducer.class);
        job2.setOutputKeyClass(NPFeatureIndex.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setNumReduceTasks(NUM_OF_REDUCERS);
        job2.setPartitionerClass(NounPairPartitioner.class);

        FileInputFormat.addInputPath(job2, CORPUS);
        FileOutputFormat.setOutputPath(job2, FeaturesVector_Output);
        job2.waitForCompletion(true);
        /* END - STEP 2*/
    }
}



