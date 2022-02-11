package jobs;
import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import common.Consts;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.model.ObjectListing;
//import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.GetObjectRequest;
//mport com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.S3Object;
//import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
//import com.amazonaws.auth.AWSCredentialsProvider;
//import com.amazonaws.auth.AWSStaticCredentialsProvider;


public class Calculate {

	public static class IdMapper extends Mapper<LongWritable, Text, Text, Text>{		

		@Override
		public void map(LongWritable filename, Text line, Context con) throws IOException, InterruptedException
		{	
			String valueSplited[] = line.toString().split("\t");
            con.write(new Text(valueSplited[0]), new Text(valueSplited[1]));
		}
	}

	public static class CalculateReducer extends Reducer<Text, Text, Text, DoubleWritable>{

		double C0 ;

		@Override
		public void setup(Context con) throws IOException {
			Regions clientRegion = Regions.US_EAST_1;
			String bucketName = con.getConfiguration().get("tempFilesPath");
			String key = "C0";
			S3Object fullObject = null;
			AmazonS3 s3Client = AmazonS3ClientBuilder
				.standard()
				.withRegion(clientRegion)
				.build();
			fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
			BufferedReader reader = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
			String line = reader.readLine();
			C0 = Long.parseLong(line);
		}

		@Override
		public void reduce(Text triplet, Iterable<Text> occurrencesList, Context con) throws IOException, InterruptedException {
            long[] occurrencesForEach = new long[5];  // C1 , N1, C2, N2, N3
            for (Text occurrences : occurrencesList) {
                String[] items = occurrences.toString().split(" ");

                if (occurrencesForEach[4] == 0) {
                    occurrencesForEach[4] = Long.parseLong(items[4]);
                }

                for (int i = 0; i < items.length - 1; i++) {
                    if (!items[i].equals("-1")) {
                        occurrencesForEach[i] = Long.parseLong(items[i]);
                        // not breaking because could be multiple counters in one occurrences (when identical words)
                    }
                }
            }

            con.write(triplet, new DoubleWritable(probability(occurrencesForEach)));
		}

        private double probability(long[] occurrencesForEach) {
            double N1 = occurrencesForEach[1];
            double N2 = occurrencesForEach[3];
            double N3 = occurrencesForEach[4];
            double C1 = occurrencesForEach[0];
            double C2 = occurrencesForEach[2]; 
            double logN2 = Math.log(N2 + 1);
            double logN3 = Math.log(N3 + 1);
            double k2 = (logN2 + 1) / (logN2 + 2);
            double k3 = (logN3 + 1) / (logN3 + 2);
            double p = k3*N3/C2 + (1-k3)*k2*N2/C1 + (1-k3)*(1-k2)*N1/C0;
            return p;
        } 
	}
	

	public static void main(String [] args) throws Exception
	{
		Configuration conf = new Configuration();
		String uuid = args[0];
		Path input = new Path(String.format("s3://%s/join-output/%s/", Consts.BUCKET, uuid));
		Path output = new Path(String.format("s3://%s/calculate-output/%s/", Consts.BUCKET, uuid));

		String tempFilesPath = Consts.BUCKET+"/tempFiles/"+uuid;
		conf.set("tempFilesPath", tempFilesPath);

		Job job = Job.getInstance(conf, "Calculate");
		job.setJarByClass(Calculate.class);
		job.setMapperClass(IdMapper.class);
		job.setReducerClass(CalculateReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
	}

}





