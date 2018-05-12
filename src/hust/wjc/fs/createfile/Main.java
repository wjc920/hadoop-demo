package hust.wjc.fs.createfile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

public class Main {

	public static void main(String[] args) throws IOException {

		int count = 1000;// 生成数量

		// 生成文件地址
		File f = new File("D:/data-all");
		if (!f.createNewFile())
			return;

		OutputStreamWriter writer = null;
		BufferedWriter bw = null;

		Random random = new Random();
		try {
			OutputStream os = new FileOutputStream(f);
			writer = new OutputStreamWriter(os);
			bw = new BufferedWriter(writer);
			int i = 0;
			while (i++ < count) {
				StringBuffer str = new StringBuffer();
				int a = random.nextInt(100);
				int b = random.nextInt(10);
				str.append(a + "\t" + b + "\n");
				bw.write(str.toString());
				bw.flush();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("finish");
	}

}
