package hust.wjc.mr.patents;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRunRefCount {
	static class PatentsRefMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private final static IntWritable count = new IntWritable(1);
		private IntWritable citationCount = new IntWritable();

		@Override
		protected void map(LongWritable key, Text value, Context out) throws IOException, InterruptedException {
			String line = value.toString();
			String[] kv = line.split("\t");
			citationCount.set((kv[1].split(",")).length);
			out.write(citationCount, count);
		}

	}

	static class PatentsRefReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context out)
				throws IOException, InterruptedException {
			int tCount = 0;
			for (IntWritable v : values) {
				tCount += v.get();
			}
			out.write(key, new IntWritable(tCount));
		}

	}

	public static void main(String[] args) {
		try {
			Job job = Job.getInstance(new Configuration());
			job.setJobName("PatentsRefCount");

			job.setJarByClass(JobRun.class);
			job.setMapperClass(PatentsRefMapper.class);
			job.setReducerClass(PatentsRefReducer.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);

			job.setNumReduceTasks(1);

			FileInputFormat.addInputPath(job, new Path("/output/patents/ref"));
			FileOutputFormat.setOutputPath(job, new Path("/output/patents/ref-count"));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
