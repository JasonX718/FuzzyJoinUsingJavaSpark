package filters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.hash.Hash;

/**
 * Implements a <i>Lossy Hash Table filter bucket=64 bits of 1/2 md5 (long = 16 characters)</i>, as defined by PHAN Thuong Cang
 */
public class LossyHashFilter extends Filter {
	/** Storage for the counting buckets */
	private long[] buckets;

	/** Default constructor - use with readFields */
	public LossyHashFilter() {}
	
	//Quyen's add function
	public LossyHashFilter(int size) {
		vectorSize = size;
	}
	
	//Quyen's add function
	public LossyHashFilter(LossyHashFilter copy) {
		super(copy.vectorSize, copy.nbHash, copy.hashType);
		this.buckets = new long[copy.buckets.length];
		for (int i=0; i<buckets.length; i++) {
			this.buckets[i] = copy.buckets[i];
		}
	}

	/**
	 * Constructor
	 * @param vectorSize The vector size of <i>this</i> filter.
	 * @param nbHash The number of hash function to consider.
	 * @param hashType type of the hashing function.
	 */
	public LossyHashFilter(int vectorSize, int nbHash, int hashType) {
		super(vectorSize, nbHash, hashType);
		buckets = new long[vectorSize];
	}

	@Override
	public void add(Key key) {
		if(key == null) {
			throw new NullPointerException("key cannot be null");
		}

		//String stringKey = new String(key.getBytes());
		//String md5 = MD5Hash.digest(stringKey).toString();
		//String md5 = MD5Hash.digest(key.getBytes()).toString();
		
		long md5 = MD5Hash.digest(key.getBytes()).halfDigest();
		
		int[] h = hash.hash(key);
		hash.clear();

		for(int i = 0; i < nbHash; i++) {
			// find the bucket and assign
			buckets[h[i]] = md5;
			//System.out.println("\n -- added a key to hash table: "+ (new String(key.getBytes())).toString()  + "="+ md5);
		}

	}

	@Override
	public void and(Filter filter) {
		if(filter == null
				|| !(filter instanceof LossyHashFilter)
//				|| filter.vectorSize != this.vectorSize
				|| filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be and-ed");
		}
		LossyHashFilter lhf = (LossyHashFilter)filter;

		for(int i = 0; i < vectorSize; i++) {
			this.buckets[i] = lhf.buckets[i];
		}
	}
	
	@Override
	public boolean membershipTest(Key key) {
		if(key == null) {
			throw new NullPointerException("Key may not be null");
		}
		
		
		long md5 = MD5Hash.digest(key.getBytes()).halfDigest();	

		int[] h = hash.hash(key);
		hash.clear();
		
		boolean result = true;
		for(int i = 0; i < nbHash; i++) {
			// find the bucket and assign
			if (md5 != buckets[h[i]] ) {
				result = false;
			}
			//System.out.println("\n -- The key value in hashtable="+ buckets[h[i]] + ", md5 of key=" + md5);
		}

		return result;
	}

	
	@Override
	public void or(Filter filter) {
		if(filter == null
				|| !(filter instanceof LossyHashFilter)
//				|| filter.vectorSize != this.vectorSize
				|| filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be and-ed");
		}
		LossyHashFilter lhf = (LossyHashFilter)filter;

		for(int i = 0; i < vectorSize; i++) {
			if(lhf.buckets[i] !=0  ){
				this.buckets[i] = lhf.buckets[i];
			}

		}
	}

	@Override
	public void xor(Filter filter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void not() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();

		for(int i = 0; i < vectorSize; i++) {
			if(i > 0) {
				res.append(" ");
			}      

			res.append(buckets[i]);
		}

		return res.toString();
	}

	
	
	// Writable

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		String str = null;
		for(int i = 0; i < vectorSize; i++) {
			str = String.valueOf(buckets[i]);
			if (str == null) {
				out.writeUTF("0");
			} else {
				//out.writeLong(buckets[i]);
				out.writeUTF(str);
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		String str;
		buckets = new long[vectorSize];
		for(int i = 0; i < vectorSize; i++) {
			str = in.readUTF();
			//buckets[i] = in.readLong();
			buckets[i] = Long.parseLong(str);
		}
	}


	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		LossyHashFilter lhf = new LossyHashFilter(100, 1 , Hash.MURMUR_HASH);  //bloomSize (bits), the number of Hashs, the type of hash function 
		String key = "Laurent and me and Philippe";
		String result = "";
		
		lhf.add(new Key(key.getBytes()));
		
		String key2 = "dddscsd";
		
		if( lhf.membershipTest(new Key(key2.getBytes())) == true) result = "yes";
		else result = "no";
		
		System.out.println("Result=" + result);
		
		System.out.println("The content of lossyHashTable=" + lhf.toString());		
		
	}


}
