package hust.wjc.mr.patents;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRun {

	static class PatentsRefMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context out) throws IOException, InterruptedException {
			String line = value.toString();
			String[] kv = line.split(",");
			out.write(new Text(kv[1]), new Text(kv[0]));
			
		}

	}

	static class PatentsRefReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context out) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (Text v : values) {
				if (sb.length() > 0)
					sb.append(",");
				sb.append(v.toString());
			}
			out.write(key, new Text(sb.toString()));
		}

	}

	public static void main(String[] args) {
		try {
			Job job = Job.getInstance(new Configuration());
			job.setJobName("PatentsRef");
			
			job.setJarByClass(JobRun.class);
			job.setMapperClass(PatentsRefMapper.class);
			job.setReducerClass(PatentsRefReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(1);
			
			FileInputFormat.addInputPath(job, new Path("/input/patents"));
			FileOutputFormat.setOutputPath(job, new Path("/output/patents/ref"));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
