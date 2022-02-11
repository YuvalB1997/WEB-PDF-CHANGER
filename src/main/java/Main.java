

import java.util.UUID;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

import common.Consts;

public class Main{

	public static void main(String[] args) {

		String uuid = UUID.randomUUID().toString();
		String inputFile = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"; //"s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"; //s3://"+Consts.BUCKET+"/4-lines 
		String inputType = "seq"; // "seq"
		boolean useCombiner = true;

		// get AWS credentials
		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

		AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
				.standard()
				.withRegion(Regions.US_EAST_1)
				.withCredentials(credentialsProvider)
				.build();

		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
				.withJar("s3://"+Consts.BUCKET+"/step-count.jar")
				.withArgs(inputFile, uuid, inputType, Boolean.toString(useCombiner)); 

		StepConfig stepConfig1 = new StepConfig()
				.withName("step-count")
				.withHadoopJarStep(hadoopJarStep1)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
				.withJar("s3n://"+Consts.BUCKET+"/step-join.jar")
				.withArgs(uuid);

		StepConfig stepConfig2 = new StepConfig()
				.withName("step-join")
				.withHadoopJarStep(hadoopJarStep2)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
				.withJar("s3n://"+Consts.BUCKET+"/step-calculate.jar")
				.withArgs(uuid);

		StepConfig stepConfig3 = new StepConfig()
				.withName("step-calculate")
				.withHadoopJarStep(hadoopJarStep3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
				.withJar("s3n://"+Consts.BUCKET+"/step-sort.jar")
				.withArgs(uuid);

		StepConfig stepConfig4 = new StepConfig()
				.withName("step-sort")
				.withHadoopJarStep(hadoopJarStep4)
				.withActionOnFailure("TERMINATE_JOB_FLOW");		
		

		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(8)
				.withMasterInstanceType(InstanceType.M4Large.toString())
				.withSlaveInstanceType(InstanceType.M4Large.toString())
				.withHadoopVersion("2.10.1")
				.withKeepJobFlowAliveWhenNoSteps(false)
				.withEc2KeyName(Consts.KEY)
				.withPlacement(new PlacementType("us-east-1a"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
				.withName("ass2")
				.withReleaseLabel("emr-5.14.0")
				.withInstances(instances)
				.withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4)
				.withLogUri("s3://"+Consts.BUCKET+"/logs/")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withServiceRole("EMR_DefaultRole");

		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId + " uuid: " + uuid);
	}
}
