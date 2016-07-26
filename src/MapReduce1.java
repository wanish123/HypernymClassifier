import java.io.*;
import java.util.*;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
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


    public static class DPMapper extends Mapper<Object, Text, DependencyPath,  NounPair>{


        private HashSet<NounPair> hypernymNounPairs = new HashSet<NounPair>();
        private static final String s3BucketName = "gw-storage-30293052";
        private static final String annotatedSetFileName = "HypernymClassifier/annotated_set.txt";

        @Override
        public void setup(Context context){
            AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
            AmazonS3 s3 = new AmazonS3Client(credentials);
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

            System.out.println("IM HERE");
            String sentence = value.toString().split("\\t")[1];
            sentence = stem(sentence);
            List<Subsentence> subsentences = extractSubsentences(sentence);
            NounPair pair;
            DependencyPath dp;

            for(Subsentence subsentence: subsentences) {
                pair = subsentence.getNounPair();
                if (hypernymNounPairs.contains(pair)) {
                    dp = subsentence.getDependencyPath();
                    context.write(dp, pair);
                }
            }

        }


        private String stem(String sentence) {
            StringBuilder sb = new StringBuilder();
            String[] parts = sentence.split(" ");
            for(String part: parts){
                Stemmer stemmer = new Stemmer();
                String[] wordInfo = part.split("/");
                String word = wordInfo[0];
                stemmer.add(word.toCharArray(), word.length());
                stemmer.stem();
                word = String.valueOf(stemmer.getResultBuffer());
                sb.append(word + "/");
                sb.append(wordInfo[1] + "/");
                sb.append(wordInfo[3] + " ");

            }
            return  sb.toString();
        }

        //TODO
        private List<Subsentence> extractSubsentences(String sentence) {
            ParseTree parseTree = new ParseTree(sentence);
            List<Subsentence> subsentences = new LinkedList<Subsentence>();
            extractSubsentences(parseTree.getRoot(),"",subsentences);
            return subsentences;
        }

        private void extractSubsentences(ParseNode node, String path, List<Subsentence> subsentences) {

            if(node != null)
                for(ParseNode child: node.getChildren())
                    extractSubsentences(child, path, subsentences);

            path += (node.getPath() + " ");
            if(node.isNoun()){
                if(!path.equals("")){
                    Subsentence sentence = new Subsentence(path);
                    subsentences.add(sentence);
                    return;
                }
            }


        }


    }

    public static class FeaturesReducer extends Reducer<DependencyPath, NounPair,DependencyPath, DependencyPath> {

        private long DPMIN;
        private String DPMIN_VAR = "DPMIN";
        private HashSet<NounPair> uniqueNPs;

        @Override
        public void setup(Context context){
            DPMIN = context.getConfiguration().getLong(DPMIN_VAR, 5);
            uniqueNPs = new HashSet<NounPair>();

        }
        public void reduce(DependencyPath dp, Iterable<NounPair> values, Context context)
                throws IOException, InterruptedException {

                Iterator<NounPair> iter = values.iterator();
                while(iter.hasNext() && uniqueNPs.size() < DPMIN)
                    uniqueNPs.add(iter.next());

            if(uniqueNPs.size() == DPMIN)
                context.write(dp, dp);

            uniqueNPs.clear();

        }
    }


    /**
     *
     *
     * args[0] is the path for the corpus, args[1] is the path for output directory
     */
    public static void main(String[] args) throws Exception{

//        if(args.length != 2)
//            throw new IllegalArgumentException("Usage: " + MapReduce1.class.getSimpleName() + " < inputPath, outputPath , pmiCounters>");

        //Should be in S3
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