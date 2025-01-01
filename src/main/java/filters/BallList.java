package filters;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;

public class BallList implements Serializable {
	private static final long serialVersionUID = 1L;
	String[] elemarr;
	ArrayList<Integer>[] balls;
	BitSet set;
	
	public BallList() {
		elemarr = new String[CONST_FILTERS.vectorsize];
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		set = new BitSet(CONST_FILTERS.vectorsize);
	}
	
	public BallList(char[] s,int d) throws IOException, InterruptedException {
		elemarr = new String[CONST_FILTERS.vectorsize];
		elemarr[0]=new String(s);
		
		//success
		BallOfRadius.generateArrBall(s, s, s.length-1, s.length, elemarr); //create universe of elements 000000-999999
		
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		for(int i=0;i<elemarr.length;i++) {
			BallOfRadius.generateBallList(elemarr[i].toCharArray(), elemarr[i].toCharArray(), s.length-1, d, balls);
		}
	}
	
	public BallList(BitSet input, char[] s,int d) throws IOException, InterruptedException {
		elemarr = new String[CONST_FILTERS.vectorsize];
		elemarr[0]=new String(s);
		
		//success
		BallOfRadius.generateArrBall(s, s, s.length-1, s.length, elemarr); //create universe of elements 000000-999999
		
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		for(int i=0;i<elemarr.length;i++) {
			BallOfRadius.generateBallList(elemarr[i].toCharArray(), elemarr[i].toCharArray(), s.length-1, d, balls);
		}
		set = new BitSet(CONST_FILTERS.vectorsize);
		set.or(input);
	}
	
	public BallList(int d) throws IOException, InterruptedException {
		elemarr = new String[CONST_FILTERS.vectorsize];
		
		//success
		//BallOfRadius.generateArrBall(s, s, s.length-1, s.length, elemarr); //create universe of elements 000000-999999
		String tmp;int l = String.valueOf(CONST_FILTERS.vectorsize).length();
		for(int i=0; i<CONST_FILTERS.vectorsize; i++) {
			tmp=String.valueOf(i);
			elemarr[i] = new String();
			for(int j=0;j<l-tmp.length()-1;j++) {
			elemarr[i] = elemarr[i].concat("0");
			}
			elemarr[i]=elemarr[i].concat(tmp);
			//System.out.println(elemarr[i]);
		}
		
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		for(int i=0;i<elemarr.length;i++) {
			BallOfRadius.generateBallList(elemarr[i].toCharArray(), elemarr[i].toCharArray(), elemarr[i].length()-1, d, balls);
		}
	}
	
	public BallList(BitSet input, int d) throws IOException, InterruptedException {
		elemarr = new String[CONST_FILTERS.vectorsize];
		
		//success
		//BallOfRadius.generateArrBall(s, s, s.length-1, s.length, elemarr); //create universe of elements 000000-999999
		String tmp;int l = String.valueOf(CONST_FILTERS.vectorsize).length();
		for(int i=0; i<CONST_FILTERS.vectorsize; i++) {
			tmp=String.valueOf(i);
			elemarr[i] = new String();
			for(int j=0;j<l-tmp.length()-1;j++) {
			elemarr[i] = elemarr[i].concat("0");
			}
			elemarr[i]=elemarr[i].concat(tmp);
			//System.out.println(elemarr[i]);
		}
		
		balls = new ArrayList[CONST_FILTERS.vectorsize];
		for(int i=0;i<elemarr.length;i++) {
			if(set.get(i))
				BallOfRadius.generateBallList(elemarr[i].toCharArray(), elemarr[i].toCharArray(), elemarr[i].length()-1, d, balls);
		}
		set = new BitSet(CONST_FILTERS.vectorsize);
		set.or(input);
	}
	
	public String getElem(int i) {
		return elemarr[i];
	}
	
	public BitSet getSet() {
		return set;
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
	
	public BallList ReadFromFile(String path) {
		BallList list = new BallList(); 
        try {
 
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            list = (BallList) objectIn.readObject();
            objectIn.close();
            fileIn.close();
            System.out.println("The Object  was succesfully read from a file");
 
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return list;
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
		for(char value : CONST.ALPHABET)
	        {
	    	if(value != s[index])
			{
			    local[index] = value;
			    //System.out.println("MapOut: " + String.valueOf(local) );
			    si=String.valueOf(local);
			    sj=String.valueOf(s);
			    int indexi = Integer.valueOf(si);
			    int indexj = Integer.valueOf(sj);
			    if(indexi>indexj && set.get(indexi) && set.get(indexj)) {
			    	if(list[indexj]==null) list[indexj]=new ArrayList<>();
			    	list[indexj].add(indexi);
			    	if(list[indexi]==null) list[indexi]=new ArrayList<>();
			    	list[indexi].add(indexj);
			    }
			    
			    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));
			  
			    if(index > 0 && d > 1)
			        generateBallList(s, local, index - 1, d - 1,list,set);
			}
	        }//end for loop
	}
    }//end generateBall
	
	public static void main(String[] args) throws IOException, InterruptedException {
		long start = System.currentTimeMillis();
		BallList list = new BallList(1);
/*		list.WriteToFile("ball.txt");
		BallList balls = new BallList();
		balls = balls.ReadFromFile("ball.txt");
*/		long end = System.currentTimeMillis();
		System.out.println("buiding time " + (end - start) / 1000 + " s");
		//System.out.println(list.getElem(6));
		ArrayList<Integer> arr = list.getBall(6);
		for(int i:arr)
			System.out.println(list.getElem(i));
	}

}
