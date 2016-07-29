import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

/**
 * Created by talwanich and gilshelef on 16/06/2016.
 */
public class RunEmrJob {

    private static final String S3_HYPERNYM_PREFIX = "s3://gw-storage-30293052/HypernymClassifier/";
    private final static String CORPUS_BIG = "input/biarcs.big.txt";
    private final static String CORPUS_SMALL = "input/biarcs.small.txt";
    private static final String CORPUS_PATH = S3_HYPERNYM_PREFIX + CORPUS_BIG;
    private static final String PROJECT_JAR_PATH = S3_HYPERNYM_PREFIX + "HypernymClassifier.jar";
    private static final String S3_HYPERNYM_OUTPUT1_PATH = S3_HYPERNYM_PREFIX + "Output1/";
    private static final String S3_HYPERNYM_OUTPUT2_PATH = S3_HYPERNYM_PREFIX + "Output2/";
    private static final String S3_HYPERNYM_OUTPUT1_DPMIN_PATH = S3_HYPERNYM_PREFIX + "Output1_";
    private static final String S3_HYPERNYM_OUTPUT2_DPMIN_PATH = S3_HYPERNYM_PREFIX + "Output2_";

    private static final String ACTION_ON_FAIL = "TERMINATE_JOB_FLOW";
    private static final String NUM_Of_REDUCERS = "10";
    private static final String HADOOP_VERSION = "2.7.2";
    private static final String PLACEMENT_TYPE = "us-east-1b";
    private static final String S3_LOG_LOCATION = S3_HYPERNYM_PREFIX + "Log/";
    private static final int    NUMBER_OF_INSTANCES = 10;
    private static final String ENDPOINT = "elasticmapreduce.us-east-1.amazonaws.com";
    private static final String DPMIN = "5";

    private static void runEmrJob(){

        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        //mapReduce.setRegion(usEast1);
        mapReduce.setEndpoint(ENDPOINT);

        /* Step 1 */
        HadoopJarStepConfig
                JarStep1 = new HadoopJarStepConfig()
                .withJar(PROJECT_JAR_PATH) // This should be a full map reduce application.
                .withMainClass("MapReduce1")
                .withArgs(CORPUS_PATH, S3_HYPERNYM_OUTPUT1_PATH, DPMIN);


        StepConfig stepConfig1 = new StepConfig()
                .withName("MapReduce1")
                .withHadoopJarStep(JarStep1)
                .withActionOnFailure(ACTION_ON_FAIL);
        /* End Step 1 */


        /* Step 2 */
        HadoopJarStepConfig
                JarStep2 = new HadoopJarStepConfig()
                .withJar(PROJECT_JAR_PATH) // This should be a full map reduce application.
                .withMainClass("MapReduce2")
                .withArgs(CORPUS_PATH, S3_HYPERNYM_OUTPUT2_PATH, NUM_Of_REDUCERS);


        StepConfig stepConfig2 = new StepConfig()
                .withName("MapReduce2")
                .withHadoopJarStep(JarStep2)
                .withActionOnFailure(ACTION_ON_FAIL);
        /* End Step 2 */


        /* Running the Instances */

        /**
         * Old type: InstanceType.M1Large.toString()
         * New type: InstanceType.M1Small.toString()
         */
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(NUMBER_OF_INSTANCES)
                .withMasterInstanceType(InstanceType.M1Small.toString())
                .withSlaveInstanceType(InstanceType.M1Small.toString())
                .withHadoopVersion(HADOOP_VERSION)
                .withEc2KeyName("wanish")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(PLACEMENT_TYPE));


        Application hadoop = new Application();
        hadoop.withName("Hadoop");

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("HypernymClassifier")
                .withInstances(instances)
                .withSteps(stepConfig1, stepConfig2)
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole")
                .withReleaseLabel("emr-4.6.0")
                .withLogUri(S3_LOG_LOCATION)
                .withApplications(hadoop);

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Run job flow with id: " + jobFlowId);

    }

    private static void runEmrJobDPmin(){

        int TRIALS_NUM = 5;
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        //mapReduce.setRegion(usEast1);
        mapReduce.setEndpoint(ENDPOINT);

        HadoopJarStepConfig[] steps = new HadoopJarStepConfig[2*TRIALS_NUM];
        StepConfig[] stepsConfigs = new StepConfig[2*TRIALS_NUM];

        String dpmin;
        int i = 0;

        for(int dp = 6; dp < 11; dp += 1) {

            dpmin = String.valueOf(dp);
            steps[i] =  new HadoopJarStepConfig()
                    .withJar(PROJECT_JAR_PATH) // This should be a full map reduce application.
                    .withMainClass("MapReduce1")
                    .withArgs(CORPUS_PATH, S3_HYPERNYM_OUTPUT1_DPMIN_PATH + dpmin + "/", dpmin);

            stepsConfigs[i] = new StepConfig()
                    .withName("MapReduce1")
                    .withHadoopJarStep(steps[i])
                    .withActionOnFailure(ACTION_ON_FAIL);

            steps[i + 1] =  new HadoopJarStepConfig()
                    .withJar(PROJECT_JAR_PATH) // This should be a full map reduce application.
                    .withMainClass("MapReduce2")
                    .withArgs(CORPUS_PATH, S3_HYPERNYM_OUTPUT2_DPMIN_PATH + dpmin + "/", NUM_Of_REDUCERS);

            stepsConfigs[i + 1] = new StepConfig()
                    .withName("MapReduce2")
                    .withHadoopJarStep(steps[i + 1])
                    .withActionOnFailure(ACTION_ON_FAIL);

            i += 2;
        }


        /* Running the Instances */

        /**
         * Old type: InstanceType.M1Large.toString()
         * New type: InstanceType.M1Small.toString()
         */
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(NUMBER_OF_INSTANCES)
                .withMasterInstanceType(InstanceType.M1Small.toString())
                .withSlaveInstanceType(InstanceType.M1Small.toString())
                .withHadoopVersion(HADOOP_VERSION)
                .withEc2KeyName("wanish")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(PLACEMENT_TYPE));


        Application hadoop = new Application();
        hadoop.withName("Hadoop");

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("HypernymClassifier_DPMIN")
                .withInstances(instances)
                .withSteps(stepsConfigs)
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole")
                .withReleaseLabel("emr-4.6.0")
                .withLogUri(S3_LOG_LOCATION);

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Run job flow with id: " + jobFlowId);

    }



    public static void main(String[] args) {
        runEmrJobDPmin();
        //runEmrJob();


    }

}
