package jobs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import common.Consts;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;






public class Sort {

	public static class SortMapper extends Mapper<LongWritable, Text, Text, Text>{		

		@Override
		public void map(LongWritable filename, Text line, Context con) throws IOException, InterruptedException
		{	
			String valueSplited[] = line.toString().split("\t");
			String newKey= valueSplited[0]+" "+valueSplited[1];
            con.write(new Text(newKey),new Text(""));
		}
	}

	
	public static class SortReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text fullname, Iterable<Text> values, Context con) throws IOException, InterruptedException {
          String parts[] = fullname.toString().split(" ");
		  String key = parts[0]+" "+parts[1]+" "+parts[2];
		  String value = parts[3];
		  con.write(new Text(key), new Text(value));
		}

	}
	

	public static class KeyValueComparator extends WritableComparator {
		protected KeyValueComparator() {
			super(Text.class, true);
		}
	
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text key1 = (Text) w1;
			Text key2 = (Text) w2;       
			String parts1[] = key1.toString().split(" ");
			String parts2[] = key2.toString().split(" ");

			for (int i = 0; i < 2; i++) {
				int comp = parts1[i].compareTo(parts2[i]);
				if (comp != 0) return comp;
			}
			return -1*Double.compare(Double.parseDouble(parts1[3]), Double.parseDouble(parts2[3]));   
		}
	}
	

	public static void main(String [] args) throws Exception
	{
		Configuration conf = new Configuration();
		String uuid = args[0];
		Path input = new Path(String.format("s3://%s/calculate-output/%s/", Consts.BUCKET, uuid));
		Path output = new Path(String.format("s3://%s/sort-output/%s/", Consts.BUCKET, uuid));

		String tempFilesPath = Consts.BUCKET+"/tempFiles/"+uuid;
		conf.set("tempFilesPath", tempFilesPath);

		Job job = Job.getInstance(conf, "Sort");
		job.setSortComparatorClass(KeyValueComparator.class);
		job.setJarByClass(Sort.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setNumReduceTasks(1); // to make it a single file
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





