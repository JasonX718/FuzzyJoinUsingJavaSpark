package filters;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;

public class BallListInt implements Serializable {
	private static final long serialVersionUID = 1L;
	ArrayList<Integer>[] balls;
	BitSet set;
	
	public BallListInt() {
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		set = new BitSet(CONST_FILTERS.vectorsize);
	}
	
	public BallListInt(int d) throws IOException, InterruptedException {

		balls = new ArrayList[CONST_FILTERS.vectorsize];
		for(int i=0;i<CONST_FILTERS.vectorsize;i++) {
			char[] s = getKey(i).toCharArray();
			BallOfRadius.generateBallList(s, s, s.length-1, d, balls);
		}
	}
	
	public BallListInt(BitSet input, int d) throws IOException, InterruptedException {

		set = new BitSet(CONST_FILTERS.vectorsize);
		set.or(input);
		
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		for(int i=0;i<CONST_FILTERS.vectorsize;i++) {
			if(set.get(i)) {
				char[] s = getKey(i).toCharArray();
				generateBallList(s, s, s.length-1, d, balls,set);
			}
		}
	}
	
	public BitSet getSet() {
		return set;
	}
	
	public boolean membershiptest(int i) {
		return set.get(i);
	}
	
	public ArrayList<Integer> getBall(int i) {
		return balls[i];
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
	
	public BallListInt ReadFromFile(String path) {
		BallListInt list = new BallListInt(); 
        try {
 
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            list = (BallListInt) objectIn.readObject();
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
	
	static void generateBallList(char[] s, char[] ms, int index, int d, ArrayList<Integer>[] list,BitSet set) throws IOException, InterruptedException
    {
	//s input string 
	//ms modified string
	//index length/2 - 1 
	//d thresolde distance to join on 
	char[] local = ms.clone();
	if(index > 0 && d > 0)
        generateBallList(s, ms, index - 1, d,list,set);
	if(d>0) {
		String si,sj;
		sj=String.valueOf(s);
		int indexi,indexj;
		indexj = Integer.valueOf(sj);
		for(char value : CONST.ALPHABET)
	        {
	    	if(value != s[index])
			{
			    local[index] = value;
			    //System.out.println("MapOut: " + String.valueOf(local) );
			    si=String.valueOf(local);
			    indexi = Integer.valueOf(si);
	//		    if(indexi>indexj && set.get(indexi) && set.get(indexj)) {
			    if(set.get(indexi) && indexi<indexj) {
			    	if(list[indexj]==null) list[indexj]=new ArrayList<>();
			    	list[indexj].add(indexi);
			    }
	//		    	if(list[indexi]==null) list[indexi]=new ArrayList<>();
	//		    	list[indexi].add(indexj);
	//		    }
			    
			    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));
			  
			    if(index > 0 && d > 1)
			        generateBallList(s, local, index - 1, d - 1,list,set);
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
	/*	BitSet b = new BitSet(CONST_FILTERS.vectorsize);
		b.set(0);b.set(8);b.set(15);b.set(6);
		BallListInt list = new BallListInt(b,1);
		
*/		
		BallListInt b = new BallListInt(1); //d=1
		long end = System.currentTimeMillis();
		System.out.println("buiding time " + (end - start) / 1000 + " s"); //buiding time 10 s
		
		b.WriteToFile("ball1.dat"); //size = 557MB
		/*ArrayList<Integer> arr = list.getBall(6);
		if(arr!=null) 
			for(int i:arr)
				System.out.println(getKey(i));*/
/*		ArrayList<Integer> arr;
		for(int i=0; i<CONST_FILTERS.vectorsize; i++) {
			if(list.getSet().get(i)) {
				System.out.println(i+ ":");
				arr = list.getBall(i);
				if(arr!=null) 
					for(int j:arr)
						System.out.println(":::"+ getKey(j));
			}
		}
*/	}

}
