import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by talwanich and gilshelef on 08/06/2016.
 */
public class MapReduce2 {

    public enum Type{True, False, Unknown}
    private static final String S3_HYPERNYM_PREFIX = "s3://gw-storage-30293052/HypernymClassifier/";
    private final static String CORPUS_BIG = "input/biarcs.big.txt";
    private final static String CORPUS_SMALL = "input/biarcs.small.txt";
    private static final String CORPUS_PATH = S3_HYPERNYM_PREFIX + CORPUS_SMALL;
    private static final String PROJECT_JAR_PATH = S3_HYPERNYM_PREFIX + "HypernymClassifier.jar";
    private static String S3_HYPERNYM_OUTPUT1_PATH;
    private static final String S3_HYPERNYM_OUTPUT2_PATH = S3_HYPERNYM_PREFIX + "Output2/";
    private static final String s3BucketName = "gw-storage-30293052";
    private static List<DependencyPath> features = new LinkedList<DependencyPath>();
    private static String FEATURES_LIST_VAR = "FEATURES_LIST_VAR";



    public static class FeatureBuilderMapper extends Mapper<Object, Text, NPFeatureCoordinate, LongWritable> {

        private HashSet<NounPair> hypernymNounPairs = new HashSet<NounPair>();
        private HashSet<NounPair> nonHypernymNounPairs = new HashSet<NounPair>();
        private String FEATURES_LIST_DIRECTORY;



        private static final String annotatedSetFileName = "HypernymClassifier/annotated_set.txt";
        private static final String S3_HYPERNYM_PREFIX = "s3://gw-storage-30293052/HypernymClassifier/";

        private AmazonS3 s3;
        @Override
        public void setup(Context context){
//            //LOCAL
            AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
            s3 = new AmazonS3Client(credentials);

            FEATURES_LIST_DIRECTORY = context.getConfiguration().get(FEATURES_LIST_VAR);

    //        s3 = new AmazonS3Client();

            initializeHyperSets();
            initializeFeaturesList();

        }

        private void initializeFeaturesList() {
            S3Object object;
            BufferedReader br;
            String line;
            DependencyPath feature;

            for(S3ObjectSummary summary : S3Objects.withPrefix(s3, s3BucketName, FEATURES_LIST_DIRECTORY)) {

                object = s3.getObject(new GetObjectRequest(s3BucketName, summary.getKey()));
                br = null;

                try {
                    br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                    while ((line = br.readLine()) != null) {
                        feature = parseDependencyPath(line);
                        features.add(feature);
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (br != null) {
                        try {
                            br.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                }
            }
        }


        private DependencyPath parseDependencyPath(String line) {

            String[] parts = line.split("\t");

            if(parts.length == 0)
                return new DependencyPath("");

            String path = "";
            try{
                path = parts[0];
            }catch (ArrayIndexOutOfBoundsException e){
                e.printStackTrace();
            }
            return new DependencyPath(path);
        }


        private void initializeHyperSets() {
            S3Object object = s3.getObject(new GetObjectRequest(s3BucketName, annotatedSetFileName));
            BufferedReader br = null;

            try {

                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));

                String sCurrentLine;

                while ((sCurrentLine = br.readLine()) != null) {
                    String[] parts = sCurrentLine.split("\\t");
                    String first = stemIt(parts[0]);
                    String second = stemIt(parts[1]);
                    NounPair nounPair = new NounPair(first, second);
                    if(parts[2].equals("True")) {
                        nounPair.setType(Type.True);
                        hypernymNounPairs.add(nounPair);
                    }
                    else {
                        nounPair.setType(Type.False);
                        nonHypernymNounPairs.add(nounPair);
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

        private String stemIt(String word) {
            Stemmer stemmer = new Stemmer();
            stemmer.add(word.toCharArray(), word.length());
            stemmer.stem();
            return stemmer.toString();

        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

            NounPair pair;
            DependencyPath dp;
            int index;
            NPFeatureCoordinate npAndIndex;

            int occurrences = getOccurrences(value);
            String sentence = value.toString().split("\\t")[1];

            if(!isLegal(sentence))
                return;

            sentence = stem(sentence);
            List<Subsentence> subsentences = extractSubsentences(sentence);


            for(Subsentence subsentence: subsentences) {
                pair = subsentence.getNounPair();
                dp = subsentence.getDependencyPath();

                if ((index = features.indexOf(dp)) != -1) {
                    pair.setType(findType(pair));
                    npAndIndex = new NPFeatureCoordinate(pair, index);
                    context.write(npAndIndex, new LongWritable(occurrences));
                }

            }
        }

        private Type findType(NounPair pair) {
            NounPair reversedPair = new NounPair(pair.getSecond(), pair.getFirst());
            if(hypernymNounPairs.contains(pair) || hypernymNounPairs.contains(reversedPair))
                return Type.True;
            if(nonHypernymNounPairs.contains(pair) || nonHypernymNounPairs.contains(reversedPair))
                return  Type.False;
            return Type.Unknown;
        }

        private int getOccurrences(Text value) {
            String[] parts = value.toString().split("\t");
            int res = 0;
            try{
                res = Integer.parseInt(parts[2]);
            }catch (NumberFormatException e){
                e.printStackTrace();
            }
            return res;
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

        private List<Subsentence> extractSubsentences(String sentence) {
            List<Subsentence> subsentences = null;
            try {
                ParseTree parseTree = new ParseTree(sentence);
                subsentences = extractSubsentences(parseTree.getRoot(), "");
            }
            catch (NullPointerException e){
                System.out.println("NullPointerException: " + sentence);

            }

            catch (ArrayIndexOutOfBoundsException e){
                System.out.println("ArrayIndexOutOfBoundsException: " + sentence);
            }

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

    public static class FeaturesVectorBuilderReducer extends Reducer<NPFeatureCoordinate,LongWritable, NPFeatureCoordinate, LongWritable> {



        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
                    }

        public void reduce(NPFeatureCoordinate key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            Iterator<LongWritable> iter = values.iterator();
            long sum = 0;
            while(iter.hasNext())
                sum += iter.next().get();

            context.write(key, new LongWritable(sum));

        }



    }


    public static void main(String[] args) throws Exception {

        if(args.length != 4)
            throw new IllegalArgumentException("Usage: " + MapReduce1.class.getSimpleName() + " < inputPath, outputPath , outputpath1, numOfReducers>");

        final Path CORPUS = new Path(args[0]);
        final Path FeaturesVector_Output = new Path(args[1]);
        final String FEATURES_LIST_DIRECTORY = args[2];
        final int NUM_OF_REDUCERS = Integer.parseInt(args[3]);

        /* STEP 2 */
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "MapReduce2");
        job2.setJarByClass(MapReduce2.class);
        job2.setMapperClass(FeatureBuilderMapper.class);
        job2.setMapOutputKeyClass(NPFeatureCoordinate.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setReducerClass(FeaturesVectorBuilderReducer.class);
        job2.setCombinerClass(FeaturesVectorBuilderReducer.class);
        job2.setOutputKeyClass(NPFeatureCoordinate.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setNumReduceTasks(NUM_OF_REDUCERS);
        job2.setPartitionerClass(NounPairPartitioner.class);

        FileInputFormat.addInputPath(job2, CORPUS);
        FileOutputFormat.setOutputPath(job2, FeaturesVector_Output);

        Configuration conf = job2.getConfiguration();
        conf.set(FEATURES_LIST_VAR, FEATURES_LIST_DIRECTORY);


        job2.waitForCompletion(true);
        /* END - STEP 2*/


    }
}



