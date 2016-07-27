import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.math3.exception.NotANumberException;
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

    public static class FeatureBuilderMapper extends Mapper<Object, Text, NPFeatureCoordinate, LongWritable> {

        private HashSet<NounPair> hypernymNounPairs = new HashSet<NounPair>();
        private HashSet<NounPair> nonHypernymNounPairs = new HashSet<NounPair>();
        private List<DependencyPath> features = new LinkedList<DependencyPath>();


        private static final String s3BucketName = "gw-storage-30293052";
        private static final String annotatedSetFileName = "HypernymClassifier/annotated_set.txt";
        private final String outputFileNameStep1 = "OutputStep1/part-r-00000";
        private AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        private AmazonS3 s3;
        @Override
        public void setup(Context context){
            s3 = new AmazonS3Client(credentials);

            initializeHyperSets();
            initializeFeaturesList();

        }

        private void initializeFeaturesList() {

            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(outputFileNameStep1));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            String line;
            DependencyPath feature;
            try {
                while ((line = br.readLine()) != null) {
                    feature = parseDependencyPath(line);
                    features.add(feature);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        private DependencyPath parseDependencyPath(String line) {
            String[] parts = line.split("\t");
            return new DependencyPath(parts[0]);
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
            NounPair customPair1 = new NounPair("custodi/NN", "control/NN");
            customPair1.setType(MapReduce2.Type.True);
            NounPair customPair2 = new NounPair("custodi/NN", "ag/NN");
            customPair2.setType(MapReduce2.Type.True);

            NounPair customPair3 = new NounPair("custodi/NN", "authors/NN");
            customPair3.setType(MapReduce2.Type.True);

            NounPair customPair4 = new NounPair("custodi/NN", "wanish/NN");
            customPair3.setType(MapReduce2.Type.True);

            NounPair customPair5 = new NounPair("authors/NN", "wanish/NN");
            customPair5.setType(MapReduce2.Type.True);

            hypernymNounPairs.add(customPair1);
            hypernymNounPairs.add(customPair2);
            hypernymNounPairs.add(customPair3);
            hypernymNounPairs.add(customPair4);
            hypernymNounPairs.add(customPair5);

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
            if(hypernymNounPairs.contains(pair))
                return Type.True;
            if(nonHypernymNounPairs.contains(pair))
                return  Type.False;
            return Type.Unknown;
        }

        private int getOccurrences(Text value) {
            String[] parts = value.toString().split("\t");
            int res = 0;
            try{
                res = Integer.parseInt(parts[2]);
            }catch (NotANumberException e){
                e.printStackTrace();
            }
            return res;
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
            ParseTree parseTree = new ParseTree(sentence);
            List<Subsentence> subsentences = new LinkedList<Subsentence>();
            extractSubsentences(parseTree.getRoot(),subsentences);
            return subsentences;
        }

        private String extractSubsentences(ParseNode node, List<Subsentence> subsentences) {

            String path = "";
            if(node != null) {

                if(node.isLeaf() && node.isNoun())
                    path = (node.getPath() + " " + path);

                for (ParseNode child : node.getChildren()) { // not a leaf
                    path = extractSubsentences(child, subsentences);
                    if(node.isNoun()){
                        path = (node.getPath() + " " + path);
                        if(isSubsentence(path)){
                            Subsentence sentence = new Subsentence(path);
                            subsentences.add(sentence);
                        }
                    }
                    else if(path.length() > 0) {
                        path = (node.getPath() + " " + path);
                    }
                }
            }



            return path;

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
        job2.waitForCompletion(true);
        /* END - STEP 2*/
    }
}



