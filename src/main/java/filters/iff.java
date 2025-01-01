package filters;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;

public class iff implements Serializable {
	private static final long serialVersionUID = 1L;
	BitSet bitset1;
	HashMap<Integer, BitSet> balls;
	
	public iff() {
		super();
	}
	
	public iff(int size) {
		super();
		bitset1 = new BitSet(size);
	}
	
	public iff(BitSet b1, HashMap<Integer, BitSet> b) {
		super();
		bitset1 = b1;
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
		if(b.isEmpty()) return null;
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
}
