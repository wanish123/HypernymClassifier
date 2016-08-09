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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by talwanich on 01/08/2016.
 */
public class MapReduceDPmin1 {



    private static final String S3_HYPERNYM_PREFIX = "s3://gw-storage-30293052/HypernymClassifier/";
    private final static String CORPUS_BIG = "input/";
    private final static String CORPUS_SMALL = "input/biarcs.small.txt";
    private static final String CORPUS_PATH = S3_HYPERNYM_PREFIX + CORPUS_SMALL;
    private static final String PROJECT_JAR_PATH = S3_HYPERNYM_PREFIX + "HypernymClassifier.jar";
    private static final String S3_HYPERNYM_OUTPUT1_PATH = S3_HYPERNYM_PREFIX + "Output1/";
    private static final String S3_HYPERNYM_OUTPUT2_PATH = S3_HYPERNYM_PREFIX + "Output2/";

    public static class DPMapper extends Mapper<Object, Text, DependencyPath,  NounPair> {


        private HashSet<NounPair> hypernymNounPairs = new HashSet<NounPair>();
        private static final String s3BucketName = "gw-storage-30293052";
        private static final String annotatedSetFileName = "HypernymClassifier/annotated_set.txt";
        private static final int MIN_PATH_LENGTH = 2;

        @Override
        public void setup(Context context){
            //LOCAL
            AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
            AmazonS3 s3 = new AmazonS3Client(credentials);
          //  AmazonS3 s3 = new AmazonS3Client();
            S3Object object = s3.getObject(new GetObjectRequest(s3BucketName, annotatedSetFileName));
            BufferedReader br = null;

            try {

                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));

                String sCurrentLine;

                while ((sCurrentLine = br.readLine()) != null) {
                    String[] parts = sCurrentLine.split("\\t");
                    if(parts[2].equals("True")) {
                        String first = stemIt(parts[0]);
                        String second = stemIt(parts[1]);
                        NounPair nounPair = new NounPair(first, second, MapReduce2.Type.True);
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

            //DEBUG
//            NounPair np1 = new NounPair(stemIt("custody"), stemIt("control"));
//            NounPair np2 = new NounPair(stemIt("custody"), stemIt("board"));
//            NounPair np3 = new NounPair(stemIt("custody"), stemIt("child"));
//            NounPair np4 = new NounPair(stemIt("custody"), stemIt("wanish"));
//            NounPair np5 = new NounPair(stemIt("authors"), stemIt("wanish"));
//            NounPair np6 = new NounPair(stemIt("custody"), stemIt("wanish"));
//            NounPair np7 = new NounPair(stemIt("custody"), stemIt("authors"));
//            NounPair np8 = new NounPair(stemIt("custody"), stemIt("age"));
//
//            hypernymNounPairs.add(np1);
//            hypernymNounPairs.add(np2);
//            hypernymNounPairs.add(np3);
//            hypernymNounPairs.add(np4);
//            hypernymNounPairs.add(np5);
//            hypernymNounPairs.add(np6);
//            hypernymNounPairs.add(np7);
//            hypernymNounPairs.add(np8);



        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String sentence = value.toString().split("\\t")[1];

            if(!isLegal(sentence))
                return;

            sentence = stem(sentence);
            List<Subsentence> subsentences = extractSubsentences(sentence);
            NounPair pair;
            DependencyPath dp;

            for(Subsentence subsentence: subsentences) {
                pair = subsentence.getNounPair();
                    dp = subsentence.getDependencyPath();
                    if(!dp.isEmpty()){
                        System.out.println("map1: emits < " + dp + " , " + pair + " >");
                        context.write(dp, pair);
                    }

            }
        }

        private boolean isLegal(String sentence) {

            String[] parts = sentence.split(" ");
            for(String part: parts){
                String[] wordInfo = part.split("/");
                if(wordInfo.length < 4) {
                    return false;
                }
                //TODO check that all parts are correct
                String word = wordInfo[0];
                String partOfSpeech = wordInfo[1];
                String headIndex = wordInfo[3];
                if(!(isWord(word) && isPartOfSpeech(partOfSpeech) && isIndex(headIndex)))
                    return false;


            }
            return true;
        }

        private boolean isIndex(String headIndex) {
            String REGEX = "^[0-9]+$";
            if(headIndex.matches(REGEX))
                return true;

            return false;        }

        private boolean isPartOfSpeech(String partOfSpeech) {
            String REGEX = "^[A-Z]+$";
            if(partOfSpeech.matches(REGEX))
                return true;

            return false;
        }

        private boolean isWord(String word) {
            String REGEX = "^[a-zA-Z]+$";
            if(word.matches(REGEX))
                return true;

            return false;
        }


        private String stem(String sentence) {
            StringBuilder sb = new StringBuilder();
            String[] parts = sentence.split(" ");
            for(String part: parts){
                String[] wordInfo = part.split("/");
                String word = wordInfo[0];

                word = stemIt(word);

                sb.append(word + "/");
                sb.append(wordInfo[1] + "/");
                sb.append(wordInfo[3] + " ");

            }
            return  sb.toString();
        }

        private String stemIt(String word) {
            Stemmer stemmer = new Stemmer();
            stemmer.add(word.toCharArray(), word.length());
            stemmer.stem();
            return stemmer.toString();

        }

        private List<Subsentence> extractSubsentences(String sentence) {
            ParseTree parseTree = new ParseTree(sentence);
            List<Subsentence> subsentences = extractSubsentences(parseTree.getRoot(),"");
            return subsentences;
        }

        private List<Subsentence> extractSubsentences(ParseNode node, String path) {

            List<Subsentence> result = new LinkedList<Subsentence>();
            List<Subsentence> childResult;
            List<Subsentence> self;

            if(path.isEmpty())
                path = node.getPath();
            else
                path = (path + " " + node.getPath());

            if(node.isNoun() && isSubsentence(path)){
                Subsentence sentence = new Subsentence(path);
                result.add(sentence);
                self = extractSubsentences(node, "");
                result.addAll(self);
            }

            for (ParseNode child : node.getChildren()) { // not a leaf
                childResult = extractSubsentences(child, path);
                result.addAll(childResult);
            }

            return result;

        }

        private boolean isSubsentence(String path) {
            boolean gil;
            String[] parts = path.split(" ");
            gil = parts.length > 1;
            parts = parts[0].split("/");
            ParseNode tmp = new ParseNode(parts[0], parts[1], 1);
            return tmp.isNoun() && gil;


        }


    }

    public static class DPReducer extends Reducer<DependencyPath, NounPair,DependencyPath, LongWritable> {

        private HashSet<NounPair> uniqueNPs;

        @Override
        public void setup(Context context){
            uniqueNPs = new HashSet<NounPair>();

        }
        public void reduce(DependencyPath dp, Iterable<NounPair> values, Context context)
                throws IOException, InterruptedException {

            Iterator<NounPair> iter = values.iterator();
            while(iter.hasNext())
                uniqueNPs.add(iter.next());


            context.write(dp, new LongWritable(uniqueNPs.size()));

            uniqueNPs.clear();

        }
    }


    /**
     *
     *
     * args[0] is the path for the corpus, args[1] is the path for output directory
     */
    public static void main(String[] args) throws Exception{

        if(args.length != 2)
            throw new IllegalArgumentException("Usage: " + MapReduce1.class.getSimpleName() + " < inputPath, outputPathStep1>");

        //Should be in S3
        final Path CORPUS = new Path(args[0]);
        final Path OUTPUT_DP = new Path(args[1]);


        /* STEP 1 */
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "MapReduceDPMmin1");
        job1.setJarByClass(MapReduce1.class);

        job1.setMapperClass(MapReduceDPmin1.DPMapper.class);

        job1.setMapOutputKeyClass(DependencyPath.class);

        job1.setMapOutputValueClass(NounPair.class);

        job1.setReducerClass(MapReduceDPmin1.DPReducer.class);

        job1.setOutputKeyClass(DependencyPath.class);

        job1.setOutputValueClass(LongWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, CORPUS);

        FileOutputFormat.setOutputPath(job1, OUTPUT_DP);

        job1.waitForCompletion(true);

        /* END - STEP 1*/




    }



}
