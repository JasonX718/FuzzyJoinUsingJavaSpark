package filters;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class fuzzyfilter implements Serializable {
	private static final long serialVersionUID = 1L;
	BitSet bitset1;
	Map<Integer, BitSet> balls;
	
	public fuzzyfilter() {
		super();
	}
	
	public fuzzyfilter(int size) {
		super();
		bitset1 = new BitSet(size);
	}
	
	public fuzzyfilter(BitSet b1, Map<Integer, BitSet> b) {
		super();
		bitset1 = new BitSet(CONST_FILTERS.vectorsize);
		bitset1.or(b1);
		balls = b;
	}
	
	public fuzzyfilter(BitSet b1, HashMap<Integer, BitSet> b) {
		super();
		bitset1 = new BitSet(CONST_FILTERS.vectorsize);
		bitset1.or(b1);
		balls = b;
	}
	
	public BitSet getset1() {
		return bitset1;
	}
	
	public BitSet getballs(int i) {
		return balls.get(i);
	}
	
	
	public static BitSet convertS2B(String s) {
		BitSet b = new BitSet(CONST_FILTERS.vectorsize);
		if(s!=null) {
			String[] arr = s.replaceAll(" ", "").split(",");
			for (String str: arr) {
				if (str.length()>0) b.set(Integer.parseInt(str));
			}
		}
		
		return b;
	}
	public static BitSet convertS2B2(String s) {
		BitSet b = new BitSet(CONST_FILTERS.vectorsize);
		if(s!=null) {
			s = s.substring(1,s.length()-1);
			String[] arr = s.replaceAll(" ", "").split(",");
			for (String str: arr) {
				if (str.length()>0) b.set(Integer.parseInt(str));
			}
		}
		
		return b;
	}
	
	public static String convertB2S(BitSet b) {
		//if(b.isEmpty()) return null;
		String str = b.toString();
		str=str.substring(1,str.length()-1);
		str = str.replaceAll(" ", "");
		return str;
	}
	
	public static String[] convertB2A(BitSet b) {
		if(b.isEmpty()) return null;
		String str = b.toString();
		str=str.substring(1,str.length()-1);
		str = str.replaceAll(" ", "");
		return str.split(",");
	}

	
	public BitSet getSimilarlementsR(String str) {
		int p = Math.abs(CONST_FILTERS.hashFunction.hash(str.getBytes())) % CONST_FILTERS.vectorsize;
	//	if(!bitset1.get(p) || !bitset2.get(p)) return balls.get(p); //only real balls of elements exist
		BitSet b = new BitSet(CONST_FILTERS.vectorsize);
		b.or(balls.get(p)); //else list include 2 list balls of 2 elements 
		b.and(bitset1);
		
		return b;
		}	
	
	public BitSet getSimilarlementsR(int p) {
		BitSet b = new BitSet(CONST_FILTERS.vectorsize);
		b.or(balls.get(p)); //else list include 2 list balls of 2 elements 
		b.and(bitset1);
		
		return b;
		}	
	
	
/*	public String getNewSimilarelementsS(String str) {
		int p = Math.abs(CONST_FILTERS.hashFunction.hash(str.getBytes())) % CONST_FILTERS.vectorsize;
		Iterator<BitSet> ite = balls.values().iterator();
		while(ite.hasNext()) {
			if(ite.next().get(p)) return String.valueOf(p);
		}
		return null;
		}	
*/	
	public static void main(String[] args) throws IOException, InterruptedException {
		BitSet b = new BitSet(10);
		b.set(0);
		b.set(9);
		int i = b.nextSetBit(0);
		System.out.println(i);
		i= b.nextSetBit(i+1);
		System.out.println(i);
		i= b.nextSetBit(i+1);
		System.out.println(i);
	}
}
