package hust.wjc.fs.cat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FSDataInputStreamCat {
	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		FSDataInputStream in = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			System.out.println();
			System.out.println("======");
			System.out.println();
			in.seek(3);
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}

}
