package hust.wjc.mr.sort;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class AllSortJob {

	static class AllSortMapper extends Mapper<Text, Text, IntWritable, Text> {
		@Override
		protected void map(Text key, Text value, Context out) throws IOException, InterruptedException {
			String line = value.toString();
			int intKey = Integer.valueOf((line.split("\t"))[0]);
			out.write(new IntWritable(intKey), value);
		}

	}

	static class AllSortReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context out)
				throws IOException, InterruptedException {
			for (Text value : values) {
				out.write(NullWritable.get(), value);
			}
		}
	}

	public static void main(String[] args) {
		try {
			Job job = Job.getInstance();
			job.setJarByClass(AllSortJob.class);

			FileInputFormat.addInputPath(job, new Path("/input/all-sort"));

			job.setMapperClass(AllSortMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setPartitionerClass(TotalOrderPartitioner.class);

			job.setReducerClass(AllSortReducer.class);
			job.setNumReduceTasks(10);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path("/output/all-sort"));

			InputSampler.Sampler<LongWritable, Text> sampler = new InputSampler.RandomSampler<LongWritable, Text>(0.1,
					100);
			TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("/partition/all-sort"));
			InputSampler.writePartitionFile(job, sampler);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
