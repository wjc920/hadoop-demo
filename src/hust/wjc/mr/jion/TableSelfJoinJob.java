package hust.wjc.mr.jion;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TableSelfJoinJob {

	static class SelfJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context out) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\t");
			out.write(new Text(fields[0]), new Text(fields[1] + "_r"));
			out.write(new Text(fields[1]), new Text(fields[0] + "_l"));
		}

	}

	static class SelfJoinReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context out) throws IOException, InterruptedException {
			List<Text> lList = new LinkedList<>();
			List<Text> rList = new LinkedList<>();

			for (Text v : values) {
				String vStr = v.toString();
				if (vStr.endsWith("_l")) {
					lList.add(new Text(vStr.substring(0, vStr.length() - 2)));
				} else {
					rList.add(new Text(vStr.substring(0, vStr.length() - 2)));
				}
			}

			for (Text l : lList) {
				for (Text r : rList) {
					out.write(l, r);
				}
			}
		}
	}

	public static void main(String[] args) {
		try {
			Job job = Job.getInstance();
			job.setJobName("Sort");
			job.setJarByClass(TableSelfJoinJob.class);

			// 1.配置MR输入输出位置
			FileInputFormat.addInputPath(job, new Path("/input/join-self/"));
			FileOutputFormat.setOutputPath(job, new Path("/output/join-self/"));

			job.setMapperClass(SelfJoinMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);


			job.setReducerClass(SelfJoinReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
