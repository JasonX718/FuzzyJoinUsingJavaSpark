package filters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.io.Writable;

public class BitSetWritable implements Writable {

	private BitSet bitset;

	public BitSetWritable() {
		bitset=new BitSet(CONST_FILTERS.vectorsize);
	}
	
	public BitSetWritable(int size) {
		bitset = new BitSet(size);
	}

	public BitSetWritable(BitSet set) {
		bitset=new BitSet(CONST_FILTERS.vectorsize);
		this.bitset.or(set);
	}

	public BitSet get() {
		return this.bitset;
	}
	
	public boolean isEmpty() {
		return bitset.isEmpty();
	}
	
	public BitSet unionSet(BitSetWritable set) {
		if(bitset==null) bitset=new BitSet(CONST_FILTERS.vectorsize);
		this.bitset.or(set.get());
		return bitset;
	}
	
	int convertB2I(String binary) throws NumberFormatException {
		return Integer.parseInt(binary,2);
	}
	
	public void addBinaryKey(String str) {
		try {
		bitset.set(convertB2I(str)% CONST_FILTERS.vectorsize);
		} catch(NumberFormatException e) {
			System.out.println("Key is not number");
		}
	}
	
	public void addStrKey(String str) {
		try {
			bitset.set(Math.abs(CONST_FILTERS.hashFunction.hash(str.getBytes()) % CONST_FILTERS.vectorsize));
		} catch(Exception e) {
			System.out.println(e);
		}
	}
	
	public void addStrKey(String str, int size) {
		try {
			bitset.set(Math.abs(CONST_FILTERS.hashFunction.hash(str.getBytes()) % size));
		} catch(Exception e) {
			System.out.println(e);
		}
	}

	public void readFields(DataInput in) throws IOException {

		long[] longs = new long[in.readInt()];
		for (int i = 0; i < longs.length; i++) {
			longs[i] = in.readLong();
		}

		bitset = BitSet.valueOf(longs);
	}

	public void write(DataOutput out) throws IOException {
		
		long[] longs = bitset.toLongArray();
		out.writeInt(longs.length);
		
		for (int i = 0; i < longs.length; i++) {
			out.writeLong(longs[i]);
		}
	
	}
	
	public void print() {
		System.out.println("bitset: ");
			for(int j=0;j<CONST_FILTERS.vectorsize;j++) {
				System.out.print((bitset.get(j)==true ? 1 : 0) + "  ");
			}
			System.out.println(bitset.toString());
	}
	
	public void print(int length) {
		System.out.println("bitset: ");
			for(int j=0;j<length;j++) {
				System.out.print((bitset.get(j)==true ? 1 : 0) + "  ");
			}
			System.out.println(bitset.toString());
	}
	

}