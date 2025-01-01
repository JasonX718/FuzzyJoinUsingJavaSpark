package filters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.hash.Hash;

/**
 * Implements a <i>Lossy Hash Table Filter bucket=128 bits of md5 (32 characters)</i>, as defined by PHAN Thuong Cang
 */
public class LossyHashStringFilter extends Filter {
	/** Storage for the counting buckets */
	private String[] buckets;

	/** Default constructor - use with readFields */
	public LossyHashStringFilter() {}

	/**
	 * Constructor
	 * @param vectorSize The vector size of <i>this</i> filter.
	 * @param nbHash The number of hash function to consider.
	 * @param hashType type of the hashing function (see
	 * {@link org.apache.hadoop.util.hash.Hash}).
	 */
	public LossyHashStringFilter(int vectorSize, int nbHash, int hashType) {
		super(vectorSize, nbHash, hashType);
		buckets = new String[vectorSize];
	}
	
	//Quyen's add function
		public LossyHashStringFilter(LossyHashStringFilter copy) {
			super(copy.vectorSize, copy.nbHash, copy.hashType);
			this.buckets = new String[copy.buckets.length];
			/*for (int i=0; i<buckets.length; i++) {
				this.buckets[i] = copy.buckets[i];
			}*/
			System.arraycopy(copy.buckets, 0, this.buckets, 0, this.buckets.length);
		}

	@Override
	public void add(Key key) {
		if(key == null) {
			throw new NullPointerException("key cannot be null");
		}

		//String stringKey = new String(key.getBytes());
		//String md5 = MD5Hash.digest(stringKey).toString();
		//String md5 = MD5Hash.digest(key.getBytes()).toString();
		
		String md5 = MD5Hash.digest(key.getBytes()).toString();
		
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
				|| !(filter instanceof LossyHashStringFilter)
				|| filter.vectorSize != this.vectorSize
				|| filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be and-ed");
		}
		LossyHashStringFilter lhf = (LossyHashStringFilter)filter;

		for(int i = 0; i < vectorSize; i++) {
			if (this.buckets[i] != null)
				this.buckets[i] = lhf.buckets[i];
		}
	}
	
	
	@Override
	public boolean membershipTest(Key key) {
		boolean result = false;
		
		if(key == null) {
			//throw new NullPointerException("Key may not be null");
			result = false;
		}
				
		else {
			String md5 = MD5Hash.digest(key.getBytes()).toString();		
	
			int[] h = hash.hash(key);
			hash.clear();
			
			for(int i = 0; i < nbHash; i++) {
				// find the bucket and assign
				if (md5.equals(buckets[h[i]])) result = true;
				//System.out.println("\n -- The value of key in hashtable="+ buckets[h[i]] + ", md5 of key =" + md5);
			}
		}
		return result;
	}

	@Override
	public void or(Filter filter) {
		if(filter == null
				|| !(filter instanceof LossyHashStringFilter)
				|| filter.vectorSize != this.vectorSize
				|| filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be and-ed");
		}
		LossyHashStringFilter lhf = (LossyHashStringFilter)filter;

		for(int i = 0; i < vectorSize; i++) {
			if(lhf.buckets[i] != null && lhf.buckets[i].length() !=0  ){
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
		for(int i = 0; i < vectorSize; i++) {
			if(buckets[i]==null) out.writeUTF("no");
			else out.writeUTF(buckets[i]);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		buckets = new String[vectorSize];
//		System.out.print("read size lhf " + vectorSize);
		for(int i = 0; i < vectorSize; i++) {
			buckets[i] = in.readUTF();
			if(buckets[i].equals("no")) buckets[i] = null;
		}
	}




	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		LossyHashStringFilter lhf = new LossyHashStringFilter(100, 1 , Hash.MURMUR_HASH);  //bloomSize (bits), the number of Hashs, the type of hash function 
		String key = "Laurent and me and Philippe";
		String result = "";
		
		lhf.add(new Key(key.getBytes()));
		
		String key2 = "dddscsd";
		
		if( lhf.membershipTest(new Key(key2.getBytes())) == true) result = "yes";
		else result = "no";
		
		System.out.println("Result=" + result);
		
		System.out.println("The content of lossyHashTable=" + lhf.toString());
		
	}


}//end class
