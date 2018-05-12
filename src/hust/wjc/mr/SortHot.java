package hust.wjc.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortHot extends WritableComparator {

	public SortHot() {
		super(KeyPair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		KeyPair o1 = (KeyPair) a;
		KeyPair o2 = (KeyPair) b;
		int res = Integer.compare(o1.getYear(), o2.getYear());
		if (res != 0) {
			return res;
		}
		return -Integer.compare(o1.getHot(), o2.getHot());
	}

}
