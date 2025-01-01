package filters;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class intersectionfuzzyfilter implements Serializable {
	private static final long serialVersionUID = 1L;
	BitSet bitset1;
	BitSet bitset2;
	Map<Integer, BitSet> balls;
	
	public intersectionfuzzyfilter() {
		super();
	}
	
	public intersectionfuzzyfilter(int size) {
		super();
		bitset1 = new BitSet(size);
		bitset2 = new BitSet(size);
	}
	
	public intersectionfuzzyfilter(BitSet b1, BitSet b2, Map<Integer, BitSet> b) {
		super();
		bitset1 = new BitSet(CONST_FILTERS.vectorsize);
		bitset2 = new BitSet(CONST_FILTERS.vectorsize);
		bitset1.or(b1);
		bitset2.or(b2);
		balls = b;
	}
	
	public intersectionfuzzyfilter(BitSet b1, BitSet b2, HashMap<Integer, BitSet> b) {
		super();
		bitset1 = new BitSet(CONST_FILTERS.vectorsize);
		bitset2 = new BitSet(CONST_FILTERS.vectorsize);
		bitset1.or(b1);
		bitset2.or(b2);
		balls = b;
	}
	
	public BitSet getset1() {
		return bitset1;
	}
	
	public BitSet getset2() {
		return bitset2;
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

	public BitSet getSimilarelements(String str,Boolean R) {
			int p = Math.abs(CONST_FILTERS.hashFunction.hash(str.getBytes())) % CONST_FILTERS.vectorsize;
		//	if(!bitset1.get(p) || !bitset2.get(p)) return balls.get(p); //only real balls of elements exist
			BitSet b = balls.get(p); //else list include 2 list balls of 2 elements 
			if(R) {
					b.and(bitset2);
					/*return b;*/
			} else { //S
					b.and(bitset1);
					/*if(!b.isEmpty()) {
						b.clear();
						b.set(p);
					}
					return b;*/
			}
			
			return b;
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
	
	public String getSimilarlementsS(String str) {
		int p = Math.abs(CONST_FILTERS.hashFunction.hash(str.getBytes())) % CONST_FILTERS.vectorsize;
		if(bitset2.get(p)) {
			return String.valueOf(p);
			}
		return null;
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
		
	}
}
