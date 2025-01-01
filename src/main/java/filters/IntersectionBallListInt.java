package filters;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;

public class IntersectionBallListInt implements Serializable {
	private static final long serialVersionUID = 1L;
	ArrayList<Integer>[] balls;
	BitSet set1;
	BitSet set2;
	
	public IntersectionBallListInt() {
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		set1 = new BitSet(CONST_FILTERS.vectorsize);
		set2 = new BitSet(CONST_FILTERS.vectorsize);
	}
	
	public IntersectionBallListInt(BitSet input1, BitSet input2) {
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		set1 = new BitSet(CONST_FILTERS.vectorsize);
		set1.or(input1);
		
		set2 = new BitSet(CONST_FILTERS.vectorsize);
		set2.or(input2);
	}
	
	public IntersectionBallListInt(int d) throws IOException, InterruptedException {

		balls = new ArrayList[CONST_FILTERS.vectorsize];
		for(int i=0;i<CONST_FILTERS.vectorsize;i++) {
			char[] s = getKey(i).toCharArray();
			BallOfRadius.generateBallList(s, s, s.length-1, d, balls);
		}
	}
	
	public IntersectionBallListInt(BitSet input1, BitSet input2, int d) throws IOException, InterruptedException {

		set1 = new BitSet(CONST_FILTERS.vectorsize);
		set1.or(input1);
		
		set2 = new BitSet(CONST_FILTERS.vectorsize);
		set2.or(input2);
		
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		for(int i=0;i<CONST_FILTERS.vectorsize;i++) {
			if(set1.get(i)) { // || set2.get(i)) {
				if(balls[i]==null) balls[i]=new ArrayList<Integer>();
				if(set2.get(i)) balls[i].add(i);
				char[] s = getKey(i).toCharArray();
				generateIntersectionBallList(s, s, s.length-1, d,i);
				//System.out.println(String.valueOf(s));
				//generateIntersectionBallList(s, s, s.length-1, d);
			}
		}
	}
	
	/*public void buildBalls(int d) throws IOException, InterruptedException {

		BitSet union = new BitSet(CONST_FILTERS.vectorsize);
		union.or(set1);
		union.or(set2);
		
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		for(int i=0;i<CONST_FILTERS.vectorsize;i++) {
			if(set1.get(i) || set2.get(i)) {
				if(balls[i]==null) balls[i]=new ArrayList<Integer>();
				balls[i].add(i);
				char[] s = getKey(i).toCharArray();
				//System.out.println(String.valueOf(s));
				generateBallList(s, s, s.length-1, d, balls,union);
			}
		}
	}*/
	
	public BitSet getSet1() {
		return set1;
	}
	
	public BitSet getSet2() {
		return set2;
	}
	
	public boolean membershiptest1(int i) {
		return set1.get(i);
	}
	
	public boolean membershiptest2(int i) {
		return set2.get(i);
	}
	
	public ArrayList<Integer> getBall(int i) {
		return balls[i];
	}
		
	public boolean testBall2(int i) {
		if(set1.get(i)) return true;
		else if(balls[i]!=null && balls[i].size()>0) {
			return true;
		} else return false;
	}
	
	public void WriteToFile(String path) {
		 
        try {
 
            FileOutputStream fileOut = new FileOutputStream(path);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(this);
            objectOut.close();
            fileOut.close();
            System.out.println("The Object  was succesfully written to a file");
 
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
	public IntersectionBallListInt ReadFromFile(String path) {
		IntersectionBallListInt list = new IntersectionBallListInt(); 
        try {
 
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            list = (IntersectionBallListInt) objectIn.readObject();
            objectIn.close();
            fileIn.close();
            System.out.println("The Object  was succesfully read from a file");
 
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return list;
    }
	
	public static String getKey(int i) {
		String tmp;int l = String.valueOf(CONST_FILTERS.vectorsize).length();
		String key=new String();
		tmp=String.valueOf(i);
		for(int j=0;j<l-tmp.length()-1;j++) {
		key = key.concat("0");
		}
		key=key.concat(tmp);
		return key;
	}
	
	static void generateIntersectionBallList(char[] s, char[] ms, int index, int d,ArrayList<Integer>[] list, BitSet input1, BitSet input2) throws IOException, InterruptedException
    {
	//s input string 
	//ms modified string
	//index length/2 - 1 
	//d thresolde distance to join on 
	char[] local = ms.clone();
	if(index > 0 && d > 0)
        generateIntersectionBallList(s, ms, index - 1, d,list,input1,input2);
	if(d>0) {
		String si,sj;
		sj=String.valueOf(s);
		int indexi,indexj;
		indexj = Integer.valueOf(sj);
		//System.out.println(sj+" 's ball");
		for(char value : CONST.ALPHABET)
	        {
	    	if(value != s[index])
			{
			    local[index] = value;
			    //System.out.println("MapOut: " + String.valueOf(local) );
			    si=String.valueOf(local);
			    //System.out.println(":::" + si);
			    indexi = Integer.valueOf(si);
	//		    if(indexi>indexj && set.get(indexi) && set.get(indexj)) {
			    if(input2.get(indexi)) {
			    	if(list[indexj]==null) list[indexj]=new ArrayList<Integer>();
			    	//System.out.println(indexj + " :: " + indexi);
			    	list[indexj].add(indexi);
			    	if(!input1.get(indexi) && list[indexi]==null) {
			    		list[indexi]=new ArrayList<Integer>();
			    		list[indexi].add(-1);
			    	}
			    }
	//		    	if(list[indexi]==null) list[indexi]=new ArrayList<>();
	//		    	list[indexi].add(indexj);
	//		    }
			    
			    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));
			  
			    if(index > 0 && d > 1)
			        generateIntersectionBallList(s, local, index - 1, d - 1,list,input1,input2);
			}
	        }//end for loop
	}
    }//end generateBall
	
	void generateIntersectionBallList(char[] s, char[] ms, int index, int d, int ps) throws IOException, InterruptedException
    {
	//s input string 
	//ms modified string
	//index length/2 - 1 
	//d thresolde distance to join on 
	char[] local = ms.clone();
	if(index > 0 && d > 0)
        generateIntersectionBallList(s, ms, index - 1, d, ps);
	if(d>0) {
		String si;//,sj;
		//sj=String.valueOf(s);
		int indexi;//,indexj;
		//indexj = Integer.valueOf(sj);
		//System.out.println(sj+" 's ball");
		for(char value : CONST.ALPHABET)
	        {
	    	if(value != s[index])
			{
			    local[index] = value;
			    //System.out.println("MapOut: " + String.valueOf(local) );
			    si=String.valueOf(local);
			    //System.out.println(":::" + si);
			    indexi = Integer.valueOf(si);
	//		    if(indexi>indexj && set.get(indexi) && set.get(indexj)) {
			    if(set2.get(indexi)) {
			    	//if(balls[ps]==null) balls[ps]=new ArrayList<Integer>();
			    	//System.out.println(indexj + " :: " + indexi);
			    	balls[ps].add(indexi);
			    	if(!set1.get(indexi) && balls[indexi]==null) {
			    		balls[indexi]=new ArrayList<Integer>();
			    		balls[indexi].add(-1);
			    	}
			    }
	//		    	if(list[indexi]==null) list[indexi]=new ArrayList<>();
	//		    	list[indexi].add(indexj);
	//		    }
			    
			    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));
			  
			    if(index > 0 && d > 1)
			        generateIntersectionBallList(s, local, index - 1, d - 1, ps);
			}
	        }//end for loop
	}
    }//end generateBall
	
	public static void main(String[] args) throws IOException, InterruptedException {
/*		long start = System.currentTimeMillis();
		BallListInt list = new BallListInt(1);
		list.WriteToFile("ball.txt");
		BallList balls = new BallList();
		balls = balls.ReadFromFile("ball.txt");
		long end = System.currentTimeMillis();
		System.out.println("buiding time " + (end - start) / 1000 + " s");
		//System.out.println(list.getElem(6));
*/		
		long start = System.currentTimeMillis();
		BitSet b = new BitSet(CONST_FILTERS.vectorsize);
		b.set(0);b.set(1);b.set(1000); b.set(1001);b.set(100);
		
		BitSet s = new BitSet(CONST_FILTERS.vectorsize);
		s.set(0);s.set(11);s.set(110);s.set(1111);
		IntersectionBallListInt list = new IntersectionBallListInt(b,s,1);
		long end = System.currentTimeMillis();
		System.out.println("buiding time " + (end - start) / 1000 + " s");
		/*ArrayList<Integer> arr = list.getBall(6);
		if(arr!=null) 
			for(int i:arr)
				System.out.println(getKey(i));*/
		System.out.println("balls: ");
		for(int i=0;i<CONST_FILTERS.vectorsize;i++) {
			if(list.balls[i]!=null) {
				System.out.println("-- "+ i + ": ");
				for(int j : list.balls[i])
					System.out.println(":::: " + j);
			}
		}
		
		System.out.println("test: ");
		ArrayList<Integer> arr;
		System.out.println(" s1 ");
		for(int i=0; i<CONST_FILTERS.vectorsize; i++) {
			if(list.getSet1().get(i)) {
				System.out.println(i+ ":");
				arr = list.balls[i];
				if(arr!=null) 
					for(int j:arr)
						System.out.println(":::"+ j);
			}
		}
		System.out.println(" s2 ");
		for(int i=0; i<CONST_FILTERS.vectorsize; i++) {
			if(list.getSet2().get(i)) {
				System.out.println(i+ ":");
				arr = list.balls[i];
				if(arr!=null) 
					for(int j:arr)
						System.out.println(":::"+ j);
			}
		}
	}

}
