package hust.wjc.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<KeyPair, Text> {

	@Override
	public int getPartition(KeyPair key, Text value, int num) {
		return (key.getYear() * 127) % num;
	}

}
