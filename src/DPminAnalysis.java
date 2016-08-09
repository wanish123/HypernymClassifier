import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.sun.tools.hat.internal.util.ArraySorter;
import com.sun.tools.javac.util.Pair;
import org.w3c.dom.css.Counter;

import java.io.*;
import java.util.*;

/**
 * Created by talwanich on 05/08/2016.
 */
public class DPminAnalysis {
    private static final String s3BucketName = "gw-storage-30293052";
    private static final String S3_HYPERNYM_OUTPUT_DP =  "HypernymClassifier/DP_/";
    private static List<String> paths = new LinkedList<String>();
    private static ArrayList<Integer> values = new ArrayList<Integer>();
    private static HashMap<Integer, Integer> histogram = new HashMap<Integer, Integer>();
    private static double average;
    private static double min;
    private static double max;


    public static void initializePathsAndValues(){
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        AmazonS3 s3 = new AmazonS3Client(credentials);

        S3Object object;
        BufferedReader br;
        String line, path;
        Integer value;


        for(S3ObjectSummary summary : S3Objects.withPrefix(s3, s3BucketName, S3_HYPERNYM_OUTPUT_DP)) {

            object = s3.getObject(new GetObjectRequest(s3BucketName, summary.getKey()));
            br = null;

            try {
                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    path = parts[0];
                    value = new Integer(String.valueOf(parts[1]));
                    paths.add(path);
                    values.add(value);
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

    public static void analyze(){
        initializePathsAndValues();
        findMinMax();
        findAverage();
//        initializeHistogram();
//        writeHistogram();
        writeValues();

    }

    private static void writeValues() {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter("data.txt", "UTF-8");

            for(Integer value: values)
                writer.print(value + ",");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        finally {
            if (writer != null) writer.close();
        }
    }

    private static void initializeHistogram() {
        for(Integer value: values){
            if(!histogram.containsKey(value))
                histogram.put(value, 1);
            else
                histogram.put(value, histogram.get(value) + 1);
        }

        System.out.println(histogram);

    }

    private static void writeHistogram() {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter("data.txt", "UTF-8");

            for(Map.Entry<Integer, Integer> value: histogram.entrySet())
                writer.println(value.getKey() + "\t" + value.getValue());

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
     finally {
            if (writer != null) writer.close();
        }
    }






    private static void findAverage() {
        double sum = 0;
        for(Integer value : values)
            sum += value.intValue();
        average = sum/values.size();
    }

    private static void findMinMax() {
        int minValue = values.get(0);
        int maxValue = values.get(0);
        int currValue;

        for(Integer value : values){
            currValue = value.intValue();
            if(currValue < minValue)
                minValue = currValue;
            if(maxValue < currValue)
                maxValue = currValue;
        }

        min = minValue;
        max = maxValue;
    }

    public static void main(String[] args) {
        analyze();
        System.out.println("min: " + min);
        System.out.println("max: " + max);
        System.out.println("average: " + average);
    }


}
