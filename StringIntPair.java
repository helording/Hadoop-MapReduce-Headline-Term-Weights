package comp9313.proj1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class StringIntPair implements WritableComparable<StringIntPair> {
	
	private String first;
	private Integer second;
	public StringIntPair() {
	}
	public StringIntPair(String first, Integer second) {
		set(first, second);
	}
	public void set(String left, Integer right) {
		first = left;
		second = right;
	}

	public String getFirst() {
		return first;
	}
	public Integer getSecond() {
		return second;
	}

	public int compareTo(StringIntPair o) {
		int cmp = first.compareTo(o.getFirst());
		if(cmp != 0){
			return cmp;
		}
		return compare(second, o.getSecond());
	}
	
	private int compare(Integer i1, Integer i2){
		if (i1 > i2) {
			return 1;
		} else if (i1 < i2) {
			return -1;
		} else {
			return 0;
		}
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		String[] ints = WritableUtils.readStringArray(in);
		first = ints[0];
		second = Integer.parseInt(ints[1]);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		String[] strings = new String[] { first, second.toString() };
		WritableUtils.writeStringArray(out, strings);
	}
	
	/*
	
	@Override
	public int hashCode() {
	 return first.hashCode();
	}*/
}