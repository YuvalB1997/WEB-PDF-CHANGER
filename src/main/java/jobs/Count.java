package jobs;



import static common.StringUtils.spaceSeparatedString;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import common.Consts;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.InputStream;
import java.util.regex.Pattern;
import java.io.ByteArrayInputStream;
import com.amazonaws.regions.Regions;


public class Count {

	public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

		private final Pattern nonCharsPattern = Pattern.compile("[^\u05D0-\u05EA ]"); // find non hebrew and non space chars 

		@Override
		public void map(LongWritable filename, Text line, Context output) throws IOException, InterruptedException
		{	
			String valueSplit[] = line.toString().split("\t");
			String ngram[] = valueSplit[0].split(" ");

			LongWritable occurences = new LongWritable(Long.parseLong(valueSplit[1]));	
			for (int i = 1; i <= ngram.length; i++) { // first word, first two words, all words
				String ngramStr = spaceSeparatedString(ngram, 0, i);
				if (!nonCharsPattern.matcher(ngramStr).find()) { // if all the strings are actual words, and not "("" for example
					output.write(new Text(ngramStr), occurences);
					if (i == 1) {
						output.write(new Text("*C0*"), occurences);
					}
				}			
			}
		}
	}


	public static class CountCombiner extends Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException
		{
			long sum = 0;
			for(LongWritable value : values) {
				sum += value.get();	
			}
			con.write(key, new LongWritable(sum));
		}
	}

	public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();


		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException
		{
			long sum = 0;
			for(LongWritable value : values) {
				sum += value.get();	
			}

			if (key.toString().equals("*C0*")) { //sum
				String path = con.getConfiguration().get("tempFilesPath"); //bucket
				String file = ""+sum;
				InputStream is = new ByteArrayInputStream( file.getBytes());
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(file.getBytes().length);
				PutObjectRequest req = new PutObjectRequest(path, "C0", is ,metadata);   
				s3.putObject(req);   
			} else {
				con.write(key, new LongWritable(sum)); 
			}
		}
	}


	public static void main(String [] args) throws Exception
	{
		Configuration conf = new Configuration();
		String uuid = args[1];
		String inputType = args[2];
		if (!inputType.equals("text") && !inputType.equals("seq")) {
			throw new IllegalArgumentException("either text or seq");
		}
		boolean useCombiner = Boolean.parseBoolean(args[3]); 
		Path input = new Path(args[0]);
		Path output = new Path(String.format("s3://%s/count-output/%s/", Consts.BUCKET, uuid));

		String tempFilesPath = Consts.BUCKET+"/tempFiles/"+uuid;
		conf.set("tempFilesPath", tempFilesPath);

		Job job = Job.getInstance(conf,"Count");
		job.setJarByClass(Count.class);
		job.setMapperClass(CountMapper.class);
		if (useCombiner) {
			job.setCombinerClass(CountCombiner.class);
		}
		job.setReducerClass(CountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(
			inputType.equals("text") ? 
			TextInputFormat.class : 
			SequenceFileInputFormat.class
		);
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}




