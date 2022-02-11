package jobs;

import static common.StringUtils.spaceSeparatedString;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import common.Consts;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class Join {

	public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text>{		

		@Override
		public void map(LongWritable filename, Text line, Context con) throws IOException, InterruptedException
		{	
			String valueSplit[] = line.toString().split("\t");
			String words[] = valueSplit[0].split(" "); // 1/2/3gram
			
			Text value = line;
			if (words.length == 3) {
				// 1 grams
				con.write(new Text(words[1] + "$"), value);
				if (!words[1].equals(words[2])) {
					con.write(new Text(words[2] + "$"), value);  
				}

				// 2 grams
				String str1 = spaceSeparatedString(words, 0, 2);
				String str2 = spaceSeparatedString(words, 1, 3);
				con.write(new Text(str1 + "$"), value);  
				if (!str1.equals(str2)) {
					con.write(new Text(str2 + "$"), value);   
				}
			} else { // 1 or 2
				con.write(new Text(valueSplit[0]  + "#"), value);
			}
		}
	}

	/**
	 * Same 1/2grams need to go to the same reducer, for example
	 * <one two#> and <one two$>
	 */
	public static class JoinPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReducers) {
			String keyStr = key.toString();
			String ngram = keyStr.substring(0, keyStr.length()-1);
			return Math.abs(ngram.hashCode()) % numReducers; 
		}  
	}

	/**
	 * For each 1/2gram, the key with "#" comes first and its value is the number of the ngram occourences
	 * The next key (by natural comparison) is the same 1/2gram with "$" and the values are all the affected 3grams.
	 * And then the cycle repeats.
	 */
	public static class JoinReducer extends Reducer<Text, Text, Text, Text>{

		private String occurrences;
		private String[] keyWords;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {	
			String keyStr = key.toString();
			if (keyStr.charAt(keyStr.length()-1) == '#') {
				String[] valueSplit = values.iterator().next().toString().split("\t");
				keyWords = valueSplit[0].split(" "); // 1/2gram
				occurrences = valueSplit[1]; 
				return;
			}
			
			// $$$
			for (Text val : values) {
				applyOnTriplet(val, con);
			}
		}

		private void applyOnTriplet(Text value, Context con) throws IOException, InterruptedException {
			String[] pair = value.toString().split("\t");
			String[] tripletWords = pair[0].split(" ");
			String tripletOccurnces = pair[1];
			String[] occurencesForEach = new String[] {"-1", "-1", "-1", "-1", tripletOccurnces};  // C1 , N1, C2, N2, N3

			if (keyWords.length == 1) { // need to compare tripletWords[1], [2]
				for (int i = 1; i < 3; i++) {
					if (keyWords[0].equals(tripletWords[i])) {
						occurencesForEach[i-1] = occurrences;
					}
				}
			} else { // words.length == 2, need to compare couples 0-1, 1-2
				for (int i = 0; i < 2; i++) {
					if (keyWords[0].equals(tripletWords[i]) && keyWords[1].equals(tripletWords[i+1])) {
						occurencesForEach[i+2] = occurrences;
					}
				}
			}
				
			Text newKey = new Text(spaceSeparatedString(tripletWords, 0, tripletWords.length));
			Text newVal = new Text(spaceSeparatedString(occurencesForEach, 0, occurencesForEach.length));
			con.write(newKey, newVal);
		}
	}
	

	public static void main(String [] args) throws Exception
	{
		Configuration conf=new Configuration();
		String uuid = args[0];
		Path input = new Path(String.format("s3://%s/count-output/%s/", Consts.BUCKET, uuid));
		Path output = new Path(String.format("s3://%s/join-output/%s/", Consts.BUCKET, uuid));

		String tempFilesPath = String.format("s3://%s/tempFiles/%s/", Consts.BUCKET, uuid);
		conf.set("tempFilesPath", tempFilesPath);

		Job job = Job.getInstance(conf, "Join");
		job.setJarByClass(Join.class);
		job.setMapperClass(JoinMapper.class);
		job.setPartitionerClass(JoinPartitioner.class);
		job.setReducerClass(JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, input); 
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true)?0:1);
	}

}





