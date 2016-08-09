import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by gilshe on 7/27/16.
 */
public class ClassifierParser {

    private static  String INPUT_DIR_NAME = "Input";
    private static final String UNKNOWN_TYPE = "Unknown";
    private static final String TRUE_TYPE = "True";
    private static final String OUTPUT_FILE_NAME = "Output";
    private static final String TXT = ".txt";
    private static boolean firstLine = true;

    private static Integer[] featureVector;

    private static int numberOfFeatures;
    private static List<Integer[]> vectors = new ArrayList();
    private static List<Integer> annotation = new ArrayList();
    private static List<String> nounPairs = new ArrayList();

    private static final String s3BucketName = "gw-storage-30293052";
    private static AmazonS3 s3;
    private static final String S3_HYPERNYM_OUTPUT1_SMALL_DPMIN_PATH = "HypernymClassifier/Output1_";
    private static final String S3_HYPERNYM_OUTPUT2_SMALL_DPMIN_PATH = "HypernymClassifier/Output2_";


    public static void parseS3(String dp){
        //LOCAL
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        s3 = new AmazonS3Client(credentials);
        S3Object object;

        numberOfFeatures = countFeatures(dp);
        System.out.println("dp: " + dp + " , numberOfFeatures " + numberOfFeatures);

        for(S3ObjectSummary summary : S3Objects.withPrefix(s3, s3BucketName, S3_HYPERNYM_OUTPUT2_SMALL_DPMIN_PATH + dp)) {
            object = s3.getObject(new GetObjectRequest(s3BucketName, summary.getKey()));
            if(summary.getKey().startsWith(S3_HYPERNYM_OUTPUT2_SMALL_DPMIN_PATH + dp + "/part-r"))
                readFileS3(object);
        }

        writeVectors(dp);

    }

    //Tal's function, don't touch!
    public static void extractFeatures(String dp) {

        String line;
        S3Object object;
        BufferedReader br;
        PrintWriter writer;
        try {
            writer = new PrintWriter("features.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return;
        }

        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        s3 = new AmazonS3Client(credentials);


        for(S3ObjectSummary summary : S3Objects.withPrefix(s3, s3BucketName, S3_HYPERNYM_OUTPUT1_SMALL_DPMIN_PATH + dp)) {

            object = s3.getObject(new GetObjectRequest(s3BucketName, summary.getKey()));
            br = null;

            try {
                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                while ((line = br.readLine()) != null) {
                    writer.println(line);
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


        if(writer != null)
            writer.close();


    }


    public static int countFeatures(String dp) {
        int counter = 0;
        S3Object object;
        BufferedReader br;

        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        s3 = new AmazonS3Client(credentials);


        for(S3ObjectSummary summary : S3Objects.withPrefix(s3, s3BucketName, S3_HYPERNYM_OUTPUT1_SMALL_DPMIN_PATH + dp)) {

            object = s3.getObject(new GetObjectRequest(s3BucketName, summary.getKey()));
            br = null;

            try {
                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                while (br.readLine() != null) {
                    counter++;
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
            return counter;

    }

    public static void parse(String dp) {

        File[] inputFiles = new File(INPUT_DIR_NAME + "_" + dp).listFiles();

        for (File inputFile : inputFiles)
            if(inputFile.getName().startsWith("part-r"))
                readFile(inputFile);
        writeVectors(dp);

    }

    private static void writeVectors(String dp) {
        BufferedWriter bw = null;
        BufferedWriter npw = null;

        try {
            bw = new BufferedWriter(new FileWriter(OUTPUT_FILE_NAME + "_" + dp + TXT));
            npw = new BufferedWriter(new FileWriter(OUTPUT_FILE_NAME + "_noun_pairs1_" + dp + TXT));
            writeData(bw, npw);
            writeAnnotations(bw);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
                try {
                    if(bw != null) bw.close();
                    if(npw != null) npw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
    }

    private static void writeAnnotations(BufferedWriter bw) throws IOException {

        assert (annotation.size() == vectors.size());
        System.out.println("annotation.size() = " + annotation.size());

        for(Integer i: annotation) {
            bw.write(i + ", ");
        }

    }

    private static void writeData(BufferedWriter bw, BufferedWriter npw) throws IOException {
        int nounPairCount = 0;
        String cnpw;
        System.out.println("number of vectors = " + vectors.size());
        for(Integer[] vector: vectors){
            cnpw = "[ ";
            npw.write(nounPairs.get(nounPairCount) + " ");
            npw.write("[ ");

            int feature = 1;
            for(Integer i: vector) {
                i = (i == null ? 0 : i);

                bw.write(i + ", ");
                cnpw += i + " ";
                npw.write(feature + " = " +i.toString() + ", ");
                feature++;
            }

          //  System.out.println("vector.length = "  + vector.length + " vector size = " + iteration);

            npw.write(" ]");
            npw.write(cnpw + "]\n");
            nounPairCount++;
            bw.write("\n");
        }
        bw.write("*\n");
    }

    private static void readFileS3(S3Object inputFile) {

        BufferedReader br = null;
        try {

            String sCurrentLine;
            br = new BufferedReader(new InputStreamReader(inputFile.getObjectContent()));

            String[] parts, wordInfoParts;
            String currentNounPair = "", currentType = TRUE_TYPE;
            int    currentValue, currentCoordinate, iteration = 1;

            while ((sCurrentLine = br.readLine()) != null) {
                if(iteration < 3){
                    iteration++;
                    continue;
                }

                else {

                    parts = sCurrentLine.split("\t"); // key, value
                    assert (parts.length == 2);

                    wordInfoParts = parts[0].split(" ");
                    assert (wordInfoParts.length == 4); // word, word, type, coordinate

                    currentValue = Integer.parseInt(parts[1]);
                    currentCoordinate = Integer.parseInt(wordInfoParts[3]);
                    String _currentType = wordInfoParts[2];
                    String _currentNounPair = wordInfoParts[0] + " " + wordInfoParts[1];

                    assert(currentCoordinate <= numberOfFeatures);

                    if(!currentNounPair.equals(_currentNounPair)){ // new NounPair vector
                        if(featureVector != null) {
                            vectors.add(featureVector);
                            annotation.add((currentType.equals(TRUE_TYPE) ? 1 : 0));
                            nounPairs.add(currentNounPair + " " + currentType);
                        }

                        currentType = _currentType;
                        currentNounPair = _currentNounPair;
                        featureVector = new Integer[numberOfFeatures];
                    }

                    featureVector[currentCoordinate] = currentValue;

                    if (currentType.equals(UNKNOWN_TYPE))
                        break;
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch(NumberFormatException e){
            e.printStackTrace();
        } catch(IndexOutOfBoundsException e){
            e.printStackTrace();
        } finally{
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            firstLine = true;
            featureVector = null;
        }
    }

    private static void readFile(File inputFile) {

        BufferedReader br = null;
        try {

            String sCurrentLine;
            br = new BufferedReader(new FileReader(inputFile));

            String[] parts, wordInfoParts;
            String currentNounPair = "", currentType = TRUE_TYPE;
            int    currentValue, currentCoordinate;


            while ((sCurrentLine = br.readLine()) != null) {
                if(firstLine)
                    extractNumberOfFeatures(sCurrentLine);

                else {

                    parts = sCurrentLine.split("\t"); // key, value
                    assert (parts.length == 2);

                    wordInfoParts = parts[0].split(" ");
                    assert (wordInfoParts.length == 4); // word, word, type, coordinate

                    currentValue = Integer.parseInt(parts[1]);
                    currentCoordinate = Integer.parseInt(wordInfoParts[3]);
                    String _currentType = wordInfoParts[2];
                    String _currentNounPair = wordInfoParts[0] + " " + wordInfoParts[1];

                    assert(currentCoordinate <= numberOfFeatures);

                    if(!currentNounPair.equals(_currentNounPair)){ // new NounPair vector
                        if(featureVector != null) {
                            vectors.add(featureVector);
                            annotation.add((currentType.equals(TRUE_TYPE) ? 1 : 0));
                            nounPairs.add(currentNounPair + " " + currentType);
                        }

                        currentType = _currentType;
                        currentNounPair = _currentNounPair;
                        featureVector = new Integer[numberOfFeatures];
                    }

                    featureVector[currentCoordinate] = currentValue;

                    if (currentType.equals(UNKNOWN_TYPE))
                        break;
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch(NumberFormatException e){
            e.printStackTrace();
        } catch(IndexOutOfBoundsException e){
            e.printStackTrace();
        } finally{
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            firstLine = true;
            featureVector = null;
        }
    }

    private static void extractNumberOfFeatures(String line) {
        String[] parts = line.split("\t");
        //String[] parts = line.split(" +");
        try{
            numberOfFeatures = Integer.parseInt(parts[1]);
            //numberOfFeatures = Integer.parseInt(parts[4]);

        }catch (NumberFormatException e){
            System.out.println("number of features is not a number!");
        }finally {
            firstLine = false;
        }

    }
}

