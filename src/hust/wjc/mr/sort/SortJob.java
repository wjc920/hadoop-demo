package hust.wjc.mr.sort;

import java.io.IOException;

import org.apache.commons.collections.Bag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortJob {

	static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context out) throws IOException, InterruptedException {

			out.write(new IntWritable(Integer.valueOf(value.toString())), new Text(""));

		}

	}

	static class SortSort extends WritableComparator {

		public SortSort() {
			super(IntWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntWritable oa = (IntWritable) a;
			IntWritable ob = (IntWritable) b;
			return Integer.compare(oa.get(), ob.get());
		}
	}

	static class SortReduce extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

		private static IntWritable posite = new IntWritable(0);

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context out)
				throws IOException, InterruptedException {
			for (Text v : values) {
				posite = new IntWritable(posite.get() + 1);
				out.write(posite, key);

			}
		}
	}

	public static void main(String[] args) {
		try {
			Job job = Job.getInstance();
			job.setJobName("Sort");
			job.setJarByClass(SortJob.class);

			// 1.配置MR输入输出位置
			FileInputFormat.addInputPath(job, new Path("/input/sort/"));
			FileOutputFormat.setOutputPath(job, new Path("/output/sort/"));

			job.setMapperClass(SortMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setSortComparatorClass(SortSort.class);

			job.setReducerClass(SortReduce.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
