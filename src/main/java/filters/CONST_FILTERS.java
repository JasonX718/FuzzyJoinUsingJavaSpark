package filters;

import java.util.ArrayList;
import java.util.BitSet;

import org.apache.hadoop.util.hash.Hash;


/**
 * Constants for filters
 * @author tttquyen
 *
 */
public class CONST_FILTERS {
	public static String tag_BF = "BF";
	public static final String filterParts = "parts";
	public static final String filtersOutputPath = "filters";
	public static final String compressNameExtension = ".filter.gz";
	public static final String compressHBNameExtension = ".hb.gz";
	public static final String compressHFFNameExtension = ".hff.gz";
	
//	public static final String bfS = "BF_S.filter.gz";
//	public static final String bfT = "BF_T.filter.gz";
	
	//public static int SizeBF = 300000000;
	public static int SizeBF = Integer.MAX_VALUE-100;
	public static int nbHashFuncBF = 8;
	
	
	public static int nbHashFuncHBT = 1;
//	public static int vectorsize = 10000000;
	public static int vectorsize = 1000000;
	public static final int hashType = Hash.MURMUR_HASH;
	public static final Hash hashFunction = Hash.getInstance(hashType);
	public static final int NBytes = (vectorsize + 7) / 8;
	
	static final byte[] bitvalues = new byte[] { (byte) 0x01, (byte) 0x02, (byte) 0x04, (byte) 0x08,
			(byte) 0x10, (byte) 0x20, (byte) 0x40, (byte) 0x80 };
	
	public static boolean membershipTestBF(BitSet bits, String key) {
		if (key == null) {
			throw new NullPointerException("key cannot be null");
		}
		for (int i = 0, initval = 0; i < CONST_FILTERS.nbHashFuncBF; i++) {
			initval = CONST_FILTERS.hashFunction.hash(key.getBytes(), initval);
			if (!bits.get(Math.abs(initval) % CONST_FILTERS.SizeBF)) {
				return false;
			}
		}
		return true;
	}
	
	public static ArrayList<Integer> hashkeyBF(String t) {
		ArrayList<Integer> out = new ArrayList<>();
		for (int i = 0, initval = 0; i < CONST_FILTERS.nbHashFuncBF; i++) {
			initval = CONST_FILTERS.hashFunction.hash(t.getBytes(), initval);
			out.add(Math.abs(initval) % CONST_FILTERS.SizeBF);
		}
		return out;
	}
	
	public static int hashKey(String str) {
		return Math.abs(CONST_FILTERS.hashFunction.hash(str.getBytes())) % CONST_FILTERS.vectorsize;
	}
	
}
