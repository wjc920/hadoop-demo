package hust.wjc.mr.patents;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRunClaimsAvg {
	static class PatentsClaimsMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context out) throws IOException, InterruptedException {
			String line = value.toString();
			String[] kv = line.split(",");
			String country = kv[4];
			String numClaims = kv[8];
			out.write(new Text(country), new Text(numClaims));
		}

	}

	static class PatentsClaimsCombine extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context out) throws IOException, InterruptedException {
			Double sum = 0.0;
			int num = 0;
			for (Text v : values) {
				String vStr = v.toString();
				if (vStr.length() < 1)
					vStr = "0.0";
				sum += Double.valueOf(vStr);
				num++;
			}
			out.write(key, new Text(sum + "," + num));
		}

	}

	static class PatentsClaimsReducer extends Reducer<Text, Text, Text, DoubleWritable> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context out) throws IOException, InterruptedException {
			Double sum = 0.0;
			int num = 0;
			for (Text v : values) {

				String str = v.toString();
				String[] strArr = str.split(",");
				sum += Double.valueOf(strArr[0]);
				num += Integer.valueOf(strArr[1]);
			}
			out.write(key, new DoubleWritable(sum / num));
		}

	}

	public static void main(String[] args) {
		try {
			Job job = Job.getInstance(new Configuration());
			job.setJobName("PatentsClaimsCount");

			job.setJarByClass(JobRun.class);
			job.setMapperClass(PatentsClaimsMapper.class);
			job.setCombinerClass(PatentsClaimsCombine.class);
			job.setReducerClass(PatentsClaimsReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			job.setNumReduceTasks(1);

			FileInputFormat.addInputPath(job, new Path("/input/patents-claims"));
			FileOutputFormat.setOutputPath(job, new Path("/output/patents-claims"));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
