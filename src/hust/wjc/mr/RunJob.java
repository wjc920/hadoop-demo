package hust.wjc.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

	static class HotMapper extends Mapper<LongWritable, Text, KeyPair, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context out) throws IOException, InterruptedException {
			String line = value.toString();
			String[] ss = line.split("\t");
			if (ss.length == 2) {
				try {
					Date d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(ss[0]);
					KeyPair outKey = new KeyPair();
					Calendar c = Calendar.getInstance();
					c.setTime(d);
					outKey.setYear(c.get(Calendar.YEAR));
					outKey.setHot(Integer.valueOf(ss[1].substring(0, ss[1].lastIndexOf("℃"))));
					out.write(outKey, value);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}
	}

	static class HotReduce extends Reducer<KeyPair, Text, KeyPair, Text> {
		@Override
		protected void reduce(KeyPair key, Iterable<Text> values, Context out)
				throws IOException, InterruptedException {
			for (Text v : values) {
				out.write(key, v);
			}
		}
	}

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			Job job = new Job();
			job.setJobName("hot");
			job.setJarByClass(RunJob.class);
			job.setMapperClass(HotMapper.class);
			job.setReducerClass(HotReduce.class);
			job.setMapOutputKeyClass(KeyPair.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setNumReduceTasks(3);
			job.setPartitionerClass(FirstPartitioner.class);
			job.setSortComparatorClass(SortHot.class);
			job.setGroupingComparatorClass(Group.class);
			
			FileInputFormat.addInputPath(job, new Path("/input/hot"));
			FileOutputFormat.setOutputPath(job, new Path("/output/hot"));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
