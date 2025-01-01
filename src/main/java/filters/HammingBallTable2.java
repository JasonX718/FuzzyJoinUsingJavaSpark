package filters;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;


public class HammingBallTable2 extends Filter implements Serializable {
	private static final long serialVersionUID = 1L;
	
	/** Hamming ball list */
//	BitSet hbs;
	String[] list;
	//int i,w;
	String tmps;
	
/*	public BitSet gethblist() {
		return hbs;
	}*/
		
	public HammingBallTable2() {
		// TODO Auto-generated constructor stub
		super();
	}
	
	public HammingBallTable2(int size) {
		super();
		vectorSize = size;
	}
	
	public HammingBallTable2(int vectorSize, int nbHash, int hashType) {
		super(vectorSize, nbHash, hashType);

//		bits = new BitSet(this.vectorSize);
//		hbs = new BitSet(this.vectorSize);
		list=new String[this.vectorSize];
/*		for(int i=0; i<this.vectorSize;i++) {
			list[i]=new String();
			//hblist[i].clear();
		}*/
	}
	
	/**
	 * create list of all Hamming balls 
	 * @param vectorSize
	 * @param nbHash
	 * @param hashType
	 * @param length : lenght of each string
	 * @param thres : max distance
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public HammingBallTable2(int vectorSize, int nbHash, int hashType, int length, int thres, boolean binary) throws IOException, InterruptedException {
		super(vectorSize, nbHash, hashType);
//		bits = new BitSet(this.vectorSize);
//		hbs = new BitSet(this.vectorSize);
		list=new String[this.vectorSize];
/*		for(i=0; i<this.vectorSize;i++) {
			list[i]=new String();
		}*/
		char[] c=new char[length];
		for(int i = 0; i<length; i++) {
			c[i]=CONST.ALPHABET[0];
		}
//		System.out.println("B2I " + String.valueOf(c));
		//this.hblist[convertB2I(String.valueOf(c))].set(convertB2I(String.valueOf(c)));
//		System.out.println(String.valueOf(c));
		
		if (binary) {
			int ps = Math.abs(Integer.parseInt(String.valueOf(c),2)) % CONST_FILTERS.vectorsize;
			if (list[ps]==null) list[ps] = new String();
			list[ps]=list[ps].concat(",").concat(String.valueOf(ps));
//			System.out.println(ps + " " + list[ps]);
			setBinaryBall(c, c, length-1, thres);
			generateBinaryBall(c, c, length-1, length, thres);
		}
		else {
			int ps = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(c).getBytes())) % CONST_FILTERS.vectorsize;
			if (list[ps]==null) list[ps] = new String();
			list[ps]=list[ps].concat(",").concat(String.valueOf(ps));
//			System.out.println(list[ps]);
			setBall(c, c, length-1, thres);
			generateBall(c, c, length-1, length, thres);
		}
		
/*		for(i=0; i<this.vectorSize;i++) {
			if (list[i].length()>0) list[i]=list[i].substring(1);
		}*/
		
	}
	
	public static void BallCalculate(String[] arr, int vectorSize, int nbHash, int hashType, int length, int thres, boolean binary, char[] ALPHABET) throws IOException, InterruptedException {
		
		char[] c=new char[length];
		for(int i = 0; i<length; i++) {
			c[i]=ALPHABET[0];
		}

		if (binary) {
			int ps = Math.abs(Integer.parseInt(String.valueOf(c),2)) % CONST_FILTERS.vectorsize;
			if (arr[ps]==null) arr[ps] = new String();
			arr[ps]=arr[ps].concat(",").concat(String.valueOf(ps));
//			System.out.println(ps + " " + list[ps]);
			setBinaryBall(arr,c, c, length-1, thres, ALPHABET);
			generateBinaryBall(arr,c, c, length-1, length, thres,ALPHABET);
		}
		else {
			int ps = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(c).getBytes())) % CONST_FILTERS.vectorsize;
			if (arr[ps]==null) arr[ps] = new String();
			arr[ps]=arr[ps].concat(",").concat(String.valueOf(ps));
//			System.out.println(list[ps]);
			setBall(arr,c, c, length-1, thres,ALPHABET);
			generateBall(arr,c, c, length-1, length, thres,ALPHABET);
		}
		
		
	}
	
public static String[] BallCalculate(int length, int thres, boolean binary, char[] ALPHABET) throws IOException, InterruptedException {
	String[] arr = new String[CONST_FILTERS.vectorsize];
		char[] c=new char[length];
		for(int i = 0; i<length; i++) {
			c[i]=ALPHABET[0];
		}

		if (binary) {
			int ps = Math.abs(Integer.parseInt(String.valueOf(c),2)) % CONST_FILTERS.vectorsize;
			if (arr[ps]==null) arr[ps] = new String();
			arr[ps]=arr[ps].concat(",").concat(String.valueOf(ps));
//			System.out.println(ps + " " + list[ps]);
			setBinaryBall(arr,c, c, length-1, thres, ALPHABET);
			generateBinaryBall(arr,c, c, length-1, length, thres,ALPHABET);
		}
		else {
			int ps = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(c).getBytes())) % CONST_FILTERS.vectorsize;
			if (arr[ps]==null) arr[ps] = new String();
			arr[ps]=arr[ps].concat(",").concat(String.valueOf(ps));
//			System.out.println(list[ps]);
			setBall(arr,c, c, length-1, thres,ALPHABET);
			generateBall(arr,c, c, length-1, length, thres,ALPHABET);
		}
		
	return arr;	
	}
	
	public static int convertB2I(String binary) throws NumberFormatException {
		return Integer.parseInt(binary,2);
	}
	
	void generateBinaryBall(char[] s, char[] ms, int index, int d, int thres) throws IOException, InterruptedException
    {
		char[] local = ms.clone();
//		setBall(s, s, index, t);
//		System.out.println(String.valueOf(s));
		if(index > 0 && d > 0) {
			//System.out.println(String.valueOf(s));
	        generateBinaryBall(s, ms, index - 1, d, thres);
		}
		for(char value : CONST.ALPHABET)
	        {
	    	if(value != s[index])
			{
			    local[index] = value;
//			    System.out.println(String.valueOf(local));
			    int ps = Math.abs(Integer.parseInt(String.valueOf(s),2)) % CONST_FILTERS.vectorsize;
			    int pl = Math.abs(Integer.parseInt(String.valueOf(local),2)) % CONST_FILTERS.vectorsize;
			    //System.out.println(this.list[ps].contains(pl));
			    tmps=String.valueOf(pl);
			    if (list[pl]==null) list[pl] = new String();
			    list[pl]=this.list[pl].concat(",").concat(String.valueOf(pl));
			    tmps=",".concat(String.valueOf(ps)).concat(",");
			    if(!this.list[ps].contains(tmps)) {
			    	list[ps]=this.list[ps].concat(",").concat(String.valueOf(ps));
				}
//			    System.out.println(ps + " " + pl + " "+ list[ps]);
//			    System.out.println("MapOut: " + String.valueOf(local) );
			    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));
			    setBinaryBall(local, local, local.length-1, thres);
			    if(index > 0 && d > 1) {
			        generateBinaryBall(s, local, index - 1, d - 1, thres);
			    }
			}
	        }//end for loop

    }//end generateBinaryBall
	
	void setBinaryBall(char[] s, char[] ms, int index, int thres) throws IOException, InterruptedException
    {
		char[] local = ms.clone();
//		int ps = Math.abs(Integer.parseInt(String.valueOf(s),2) % CONSTANTS_FILTERS.vectorsize);
//		int pl = Math.abs(Integer.parseInt(String.valueOf(local),2) % CONSTANTS_FILTERS.vectorsize);
//		String tmps=String.valueOf(pl);
//		System.out.println(String.valueOf(local));
//		if(!this.list[ps].contains(String.valueOf(ps))) {
//		this.list[ps]=this.list[ps].concat(",").concat(String.valueOf(ps));
//		}
		if(index > 0 && thres > 0)
	        setBinaryBall(s, ms, index - 1, thres);

		for(char value : CONST.ALPHABET)
	        {
	    	if(value != s[index])
			{
			    local[index] = value;		  
//			    System.out.println("ball: " + String.valueOf(local) + " ps " + ps + " pl " + pl );
			    int ps = Math.abs(Integer.parseInt(String.valueOf(s),2)) % CONST_FILTERS.vectorsize;
			    String pl = String.valueOf(Math.abs(Integer.parseInt(String.valueOf(local),2)) % CONST_FILTERS.vectorsize);
			    tmps = ",".concat(pl).concat(",");
			    if(!this.list[ps].contains(tmps)) {
			    	list[ps]=this.list[ps].concat(",").concat(pl);
				}
//			    System.out.println(ps + " " + pl + " "+ list[ps]);
//			    System.out.println("\t" + String.valueOf(local));
	/*			if(!this.list[ps].contains(tmps)) {
					this.list[ps].concat(",").concat(tmps);
				}
	*/		    if(index > 0 && thres > 1)
			        setBinaryBall(s, local, index - 1, thres - 1);
			}
	        }//end for loop

    }//end setBinaryBall
	
	void setBall(char[] s, char[] ms, int index, int thres) throws IOException, InterruptedException
    {
	//s input string 
	//ms modified string
	//d distance to join on 
	//this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(s)));
	char[] local = ms.clone();
	/*	int ps = Math.abs(CONSTANTS_FILTERS.hashFunction.hash(String.valueOf(s).getBytes()) % CONSTANTS_FILTERS.vectorsize);
	int pl = Math.abs(CONSTANTS_FILTERS.hashFunction.hash(String.valueOf(local).getBytes()) % CONSTANTS_FILTERS.vectorsize);
	String tmps=String.valueOf(pl);*/
//	System.out.println(String.valueOf(local));
/*	if(!this.list[ps].contains(tmps)) {
		this.list[ps].concat(",").concat(tmps);
	}
*/	if(index > 0 && thres > 0)
        setBall(s, ms, index - 1, thres);

	for(char value : CONST.ALPHABET)
        {
    	if(value != s[index])
		{
		    local[index] = value;		    
		    int ps = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(s).getBytes())) % CONST_FILTERS.vectorsize;
		    String pl = String.valueOf(Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(local).getBytes())) % CONST_FILTERS.vectorsize);
		    tmps = ",".concat(pl).concat(",");
		    if(!this.list[ps].contains(tmps)) {
		    	list[ps]=this.list[ps].concat(",").concat(pl);
			}
//		    System.out.println("\t" + String.valueOf(local));
/*			if(!this.list[ps].contains(tmps)) {
				this.list[ps].concat(",").concat(tmps);
			}
*/		    if(index > 0 && thres > 1)
		        setBall(s, local, index - 1, thres - 1);
		}
        }//end for loop

    }//end setBall
	
	void generateBall(char[] s, char[] ms, int index, int d, int thres) throws IOException, InterruptedException
    {
	//s input string 
	//ms modified string
	//d 
	//t thresolde distance to join on 
	//this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(s)));
	char[] local = ms.clone();
//	setBall(s, s, index, t);
//	System.out.println(String.valueOf(s));
	if(index > 0 && d > 0) {
		//System.out.println(String.valueOf(s));
        generateBall(s, ms, index - 1, d, thres);
	}
	for(char value : CONST.ALPHABET)
        {
    	if(value != s[index])
		{
		    local[index] = value;
		    int ps = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(s).getBytes())) % CONST_FILTERS.vectorsize;
		    int pl = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(local).getBytes())) % CONST_FILTERS.vectorsize;
		    tmps=String.valueOf(pl);
		    if (list[pl]==null) list[pl] = new String();
		    list[pl]=this.list[pl].concat(",").concat(String.valueOf(pl));
		    tmps=",".concat(String.valueOf(ps)).concat(",");
		    if(!this.list[ps].contains(tmps)) {
		    	list[ps]=this.list[ps].concat(",").concat(String.valueOf(ps));
			}
//		    System.out.println(String.valueOf(local));
//		    System.out.println("MapOut: " + String.valueOf(local) );
		    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));
		    setBall(local, local, local.length-1, thres);
		    if(index > 0 && d > 1) {
		        generateBall(s, local, index - 1, d - 1, thres);
		    }
		}
        }//end for loop

    }//end generateBall
	
	public static void generateBinaryBall(String[] arr,char[] s, char[] ms, int index, int d, int thres,char[] ALPHABET) throws IOException, InterruptedException
    {
		char[] local = ms.clone();
		String tmps;
//		setBall(s, s, index, t);
//		System.out.println(String.valueOf(s));
		if(index > 0 && d > 0) {
			//System.out.println(String.valueOf(s));
	        generateBinaryBall(arr, s, ms, index - 1, d, thres,ALPHABET);
		}
		for(char value : ALPHABET)
	        {
	    	if(value != s[index])
			{
			    local[index] = value;
//			    System.out.println(String.valueOf(local));
			    int ps = Math.abs(Integer.parseInt(String.valueOf(s),2)) % CONST_FILTERS.vectorsize;
			    int pl = Math.abs(Integer.parseInt(String.valueOf(local),2)) % CONST_FILTERS.vectorsize;
			    //System.out.println(this.list[ps].contains(pl));
			    tmps=String.valueOf(pl);
			    if (arr[pl]==null) arr[pl] = new String();
			    arr[pl]=arr[pl].concat(",").concat(String.valueOf(pl));
			    tmps=",".concat(String.valueOf(ps)).concat(",");
			    if(!arr[ps].contains(tmps)) {
			    	arr[ps]=arr[ps].concat(",").concat(String.valueOf(ps));
				}
//			    System.out.println(ps + " " + pl + " "+ list[ps]);
//			    System.out.println("MapOut: " + String.valueOf(local) );
			    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));
			    setBinaryBall(arr,local, local, local.length-1, thres,ALPHABET);
			    if(index > 0 && d > 1) {
			        generateBinaryBall(arr,s, local, index - 1, d - 1, thres,ALPHABET);
			    }
			}
	        }//end for loop

    }//end generateBinaryBall
	
	public static void setBinaryBall(String[] arr, char[] s, char[] ms, int index, int thres,char[] ALPHABET) throws IOException, InterruptedException
    {
		char[] local = ms.clone();
		String tmps;
//		int ps = Math.abs(Integer.parseInt(String.valueOf(s),2) % CONSTANTS_FILTERS.vectorsize);
//		int pl = Math.abs(Integer.parseInt(String.valueOf(local),2) % CONSTANTS_FILTERS.vectorsize);
//		String tmps=String.valueOf(pl);
//		System.out.println(String.valueOf(local));
//		if(!this.list[ps].contains(String.valueOf(ps))) {
//		this.list[ps]=this.list[ps].concat(",").concat(String.valueOf(ps));
//		}
		if(index > 0 && thres > 0)
	        setBinaryBall(arr, s, ms, index - 1, thres,ALPHABET);

		for(char value : ALPHABET)
	        {
	    	if(value != s[index])
			{
			    local[index] = value;		  
//			    System.out.println("ball: " + String.valueOf(local) + " ps " + ps + " pl " + pl );
			    int ps = Math.abs(Integer.parseInt(String.valueOf(s),2)) % CONST_FILTERS.vectorsize;
			    String pl = String.valueOf(Math.abs(Integer.parseInt(String.valueOf(local),2)) % CONST_FILTERS.vectorsize);
			    tmps = ",".concat(pl).concat(",");
			    if(!arr[ps].contains(tmps)) {
			    	arr[ps]=arr[ps].concat(",").concat(pl);
				}
//			    System.out.println(ps + " " + pl + " "+ list[ps]);
//			    System.out.println("\t" + String.valueOf(local));
	/*			if(!this.list[ps].contains(tmps)) {
					this.list[ps].concat(",").concat(tmps);
				}
	*/		    if(index > 0 && thres > 1)
			        setBinaryBall(arr, s, local, index - 1, thres - 1,ALPHABET);
			}
	        }//end for loop

    }//end setBinaryBall
	
	public static void setBall(String[] arr, char[] s, char[] ms, int index, int thres,char[] ALPHABET) throws IOException, InterruptedException
    {
	//s input string 
	//ms modified string
	//d distance to join on 
	char[] local = ms.clone();
	String tmps;
	if(index > 0 && thres > 0)
        setBall(arr, s, ms, index - 1, thres,ALPHABET);

	for(char value : ALPHABET)
        {
    	if(value != s[index])
		{
		    local[index] = value;		    
		    int ps = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(s).getBytes())) % CONST_FILTERS.vectorsize;
		    String pl = String.valueOf(Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(local).getBytes())) % CONST_FILTERS.vectorsize);
		    tmps = ",".concat(pl).concat(",");
		    if(!arr[ps].contains(tmps)) {
		    	arr[ps]=arr[ps].concat(",").concat(pl);
			}
		    if(index > 0 && thres > 1)
		        setBall(arr,s, local, index - 1, thres - 1,ALPHABET);
		}
        }//end for loop

    }//end setBall
	
	public static void generateBall(String[] arr,char[] s, char[] ms, int index, int d, int thres,char[] ALPHABET) throws IOException, InterruptedException
    {
	//s input string 
	//ms modified string
	//d 
	//t thresolde distance to join on 
	//this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(s)));
	char[] local = ms.clone();
	String tmps;
//	setBall(s, s, index, t);
//	System.out.println(String.valueOf(s));
	if(index > 0 && d > 0) {
		//System.out.println(String.valueOf(s));
        generateBall(arr,s, ms, index - 1, d, thres,ALPHABET);
	}
	for(char value : ALPHABET)
        {
    	if(value != s[index])
		{
		    local[index] = value;
		    int ps = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(s).getBytes())) % CONST_FILTERS.vectorsize;
		    int pl = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(local).getBytes())) % CONST_FILTERS.vectorsize;
		    tmps=String.valueOf(pl);
		    if (arr[pl]==null) arr[pl] = new String();
		    arr[pl]=arr[pl].concat(",").concat(String.valueOf(pl));
		    tmps=",".concat(String.valueOf(ps)).concat(",");
		    if(!arr[ps].contains(tmps)) {
		    	arr[ps]=arr[ps].concat(",").concat(String.valueOf(ps));
			}
//		    System.out.println(String.valueOf(local));
//		    System.out.println("MapOut: " + String.valueOf(local) );
		    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));
		    setBall(arr,local, local, local.length-1, thres,ALPHABET);
		    if(index > 0 && d > 1) {
		        generateBall(arr,s, local, index - 1, d - 1, thres,ALPHABET);
		    }
		}
        }//end for loop

    }//end generateBall
	
	public void printhblist() {
		System.out.println("hblist: ");
//		System.out.println(hbs.toString());
		for (int i=0;i<list.length;i++) {
			//System.out.println("hb " + i + " " + this.vectorSize);
			System.out.println(i + "\t" + list[i]);
//			for(int j=0;j<this.vectorSize;j++) {
//				System.out.print((hbs[i].get(j)==true ? 1 : 0) + "  ");
//			}
//			System.out.println(hbs[i].toString());
		}
	}
	
	public HammingBallTable2(HammingBallTable2 hbt) {
		super(hbt.vectorSize, hbt.nbHash, hbt.hashType);
//		bits = new BitSet(hbt.vectorSize);
//		bits.or(hbt.bits);
//		hbs = new BitSet(hbt.vectorSize);
		for(int i=0; i<hbt.vectorSize;i++) {
			list[i]=new String(hbt.list[i]);
		}
		
	}
		

	@Override
	public void add(Key key) {
		// TODO Auto-generated method stub
		if (key == null || key.toString().length() <= 0) {
			throw new NullPointerException("key cannot be null");
		}

//		int[] h = hash.hash(key);
//		hash.clear();

		for (int i = 0; i < nbHash; i++) {
//			bits.set(h[i]);
		}
		
	}
	
	public void addbinarykey(String key) {
		// TODO Auto-generated method stub
		if (key == null || key.toString().length() <= 0) {
			throw new NullPointerException("key cannot be null");
		}

//			bits.set(convertB2I(key));
		
	}
	
	/**
	 * for spark accumulator
	 * @param hbt
	 * @return
	 */
	public HammingBallTable2 add(HammingBallTable2 hbt) {
//		bits.or(hbt.bits);
//		hbs.or(hbt.hbs);
		String tmps;
		String[] tmpstrarr;
		BitSet tmpset = new BitSet(vectorSize);
		for(int i=0; i<this.vectorSize;i++) {
			if (list[i]!=null) {
				tmpstrarr=hbt.list[i].split(",");
				for(int w=0;w<tmpstrarr.length;w++) {
					tmpset.set(Integer.valueOf(tmpstrarr[w]));
				}
				tmpstrarr=list[i].split(",");
				for(int w=0;w<tmpstrarr.length;w++) {
					tmpset.set(Integer.valueOf(tmpstrarr[w]));
				}
				tmps=tmpset.toString();
				list[i]=tmps.substring(1, tmps.length()-2);
			}
		}
		return this;
		
	}

	@Override
	public boolean membershipTest(Key key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void and(Filter filter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void or(Filter filter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void xor(Filter filter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void not() {
		// TODO Auto-generated method stub
		
	}
	
	/* @return number of bytes needed to hold bit vector */
/*	private int getNBytes() {
		return (vectorSize + 7) / 8;
	}
*/	
	// Writable



	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		HammingBallTable2 hb;
		//hb = new HammingBallTable(16,1,1);
		//hb.printhblist();
		hb = new HammingBallTable2(16, 1, 1, 4, 1,true);
		hb.printhblist();
/*		System.out.println("abc".compareTo("abd"));
		
		System.out.println(pumakey.getKey("aaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbb,entryNum06508507188S,entryNum08421661227S,entryNum12528200271S,entryNum12827535611,entryNum21378520837,entryNum22074184554,entryNum28657162272,entryNum35731013003,entryNum37188241706,entryNum52133043086,entryNum57381064686,entryNum60863715302,entryNum62658034622,entryNum64455732587,entryNum65160463245,entryNum66853532055,entryNum72411264342,entryNum74327856737,entryNum83162534042,entryNum83406755802,entryNum83748628203,entryNum86374830306,entryNum87416075813", 5));
		
		System.out.println(pumakey.getDistanceBetween("entryNum06508507188S", "entryNum08421661227S"));
*/		
		//HammingBallTable2 hbt = new HammingBallTable2(Integer.MAX_VALUE/4,1,CONSTANTS_FILTERS.hashType,11,1,false);
/*		System.out.println("00000000000");
		setBall("00000000000".toCharArray(), "00000000000".toCharArray(), 10, 1);
		generateBall("00000000000".toCharArray(), "00000000000".toCharArray(), 10, 11, 1);
*/		//setBall("00000000000".toCharArray(), "00000000000".toCharArray(), 10, 1);
		
/*		System.out.println("0000");
		setBinaryBall("0000".toCharArray(), "0000".toCharArray(), 3, 1);
		generateBinaryBall("0000".toCharArray(), "0000".toCharArray(), 3, 4, 1);
*/		
	}

}
