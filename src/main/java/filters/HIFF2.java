package filters;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class HIFF2 extends HammingBallTable2{
	BitSet bitset1;
	BitSet bitset2;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public HIFF2() {
		super();
	}
	
	public HIFF2(int size) {
		super();
		vectorSize=size;
		bitset1 = new BitSet(size);
		bitset2 = new BitSet(size);
	}
	
	public BitSet getset1() {
		return bitset1;
	}
	
	public BitSet getset2() {
		return bitset2;
	}
	
	public String[] getballs() {
		return getballs();
	}
	
	public void set(BitSet set1, BitSet set2, String[] setarr) {
		vectorSize=setarr.length;
		bitset1=new BitSet(vectorSize);
		bitset2=new BitSet(vectorSize);
		bitset1.or(set1);
		bitset2.or(set2);
		list = setarr;
	}
	public HIFF2(BitSet set1, BitSet set2, String[] setarr) {
		super();
		vectorSize=setarr.length;
		bitset1=new BitSet(vectorSize);
		bitset2=new BitSet(vectorSize);
		bitset1.or(set1);
		bitset2.or(set2);
//		System.out.println(hbs.toString());
		String tmps;
		this.list = new String[this.vectorSize];
		BitSet b;
		for(int i=0; i<this.vectorSize;i++) {
//			System.out.println(hbt.list[i]);
			if(set1.get(i)==false && set2.get(i)==false) {
				list[i]=null;
			}
			else {
//				System.out.println("arr " + i + " " + setarr[i]);
				b=convertS2B(setarr[i]);
				
				if (set1.get(i)==false) {
					b.and(set1);
					/*tmps = b.toString();
					if(tmps.length()>2) list[i]=new String(tmps.substring(1, tmps.length()-1));*/
				} 
				else if (set2.get(i)==false) {
					b.and(set2);
					/*tmps = b.toString();
					if(tmps.length()>2) list[i]=new String(tmps.substring(1, tmps.length()-1));*/
				} 
				else {
					BitSet tmp = new BitSet(CONST_FILTERS.vectorsize);
					tmp.or(set1);
					tmp.or(set2);
					b.and(tmp);
					/*tmps = b.toString();
					if(tmps.length()>2) list[i]=new String(tmps.substring(1, tmps.length()-1));*/
				}
				tmps = convertB2S(b);
				if(tmps!=null) list[i] = new String(tmps);
			}
		}
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
	
	public static String convertB2S(BitSet b) {
		if(b.isEmpty()) return null;
		String str = b.toString();
		str=str.substring(1,str.length()-1);
		str = str.replaceAll(" ", "");
		return str;
	}
	
	public HIFF2(BitSet set, HammingBallTable2 hbt) {
		super();
		vectorSize=hbt.vectorSize;
		bitset1=new BitSet(vectorSize);
		bitset1.or(set);
//		System.out.println(hbs.toString());
		String tmps;
		this.list = new String[this.vectorSize];
		BitSet b;
		for(int i=0; i<this.vectorSize;i++) {
//			System.out.println(hbt.list[i]);
			b=convertS2B(hbt.list[i]);
			
			b.and(set);
//			System.out.println(b.toString());
			tmps = b.toString();
			if(tmps.length()>2) list[i]=new String(tmps.substring(1, tmps.length()-1));
//			System.out.println(list[i]);
		}
	}
	
	

	@Override
	public void add(Key key) {
		// TODO Auto-generated method stub
		
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
	// Writable

	@Override
	public void write(DataOutput out) throws IOException {
		//super.write(out);
/*		byte[] bytes;
		bytes = new byte[CONSTANTS_FILTERS.NBytes];
		for (int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
			if (bitIndex == 8) {
				bitIndex = 0;
				byteIndex++;
			}
			if (bitIndex == 0) {
				bytes[byteIndex] = 0;
			}
			if (hbs.get(i)) {
				bytes[byteIndex] |= CONSTANTS_FILTERS.bitvalues[bitIndex];
			}
		}
		out.write(bytes);*/
		tmps=bitset1.toString();
		out.writeBytes(tmps.substring(1,tmps.length()-1));
		out.writeBytes("\n");
		tmps=bitset2.toString();
		out.writeBytes(tmps.substring(1,tmps.length()-1));
		for (int w = 0; w<vectorSize; w++) {
			out.writeBytes("\n");
			if(list[w] == null) 
				out.writeBytes("") ;
			else 
				out.writeBytes(list[w]);
			
			//out.write(bytes);
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		//super.readFields(in);
		vectorSize = CONST_FILTERS.vectorsize;
		nbHash = 1;
		bitset1=new BitSet(this.vectorSize);
		bitset2=new BitSet(this.vectorSize);
/*		byte[] bytes = new byte[CONSTANTS_FILTERS.NBytes];
		in.readFully(bytes);
		for (int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
			if (bitIndex == 8) {
				bitIndex = 0;
				byteIndex++;
			}
			if ((bytes[byteIndex] & CONSTANTS_FILTERS.bitvalues[bitIndex]) != 0) {
				hbs.set(i);
			}
		}*/
		tmps = in.readLine();
		bitset1 = convertS2B(tmps);
		tmps = in.readLine();
		bitset2 = convertS2B(tmps);
		list=new String[vectorSize];
//		System.out.println(hbs.toString());
		for (int w = 0; w<vectorSize; w++) {
			//list[w] = new String();
			//byte[] bytes = new byte[getNBytes()];
			//in.readFully(bytes);
			tmps=in.readLine();
//			System.out.println(tmps);
			if (tmps!=null) 
				list[w] = new String(tmps);
			else	list[w] = null;
		}
	}
	
	// Compress and save the HammingBallTable to a file.hb.gz on HDFS
	public void compressAndSaveFilterToHDFS(String hdfsCompressionFilePathName, Configuration conf)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// String hdfsUri = "hdfs://localhost:9000/"; String hdfsUri = "file:/";
		// String hdfsCompressionFilePathName = HB_PATH + "/" +
		// hbName + ".hb.gz";
		// //System.out.println("\n >> compressAndSaveFilter(), Hdfs
		// compressioned filter file=" + hdfsCompressionFilePathName);

		// Creating a HDFS compression DataOutputStream associated with a
		// (hdfs_file.codec, conf)
		FileCompressor fileCompressor = new FileCompressor();
		DataOutputStream hdfsCompOutStream = fileCompressor.createHdfsCompOutStream(hdfsCompressionFilePathName, conf);

		// Saving the bloom filter to the HDFS compression DataOutputStream
		this.write(hdfsCompOutStream);

		// Closing the HDFS compression DataOutputStream
		fileCompressor.close();
		hdfsCompOutStream.close();
	}

	// Load a Local Compression Filter File into the hamming ball table
	public boolean getLocalCompressionFilterFile(String localCompressionFilePathName, Configuration conf)
			throws IOException {
		boolean result = false;
		// FSDataInputStream bfFileInputStream=null;
		DataInputStream bfFileInputStream = null;

		// String localFilePathName = localCompressionFilePathName.substring(0,
		// localCompressionFilePathName.lastIndexOf(".")) ;
		/*String localFilePathName = localCompressionFilePathName.substring(0,
				localCompressionFilePathName.lastIndexOf(CONSTANTS_FILTERS.compressHBNameExtension));*/
		String localFilePathName = localCompressionFilePathName.concat(".extract");
		// Uncompress a local compressed filter file
		FileDecompressor.decompressLocalFile(localCompressionFilePathName, localFilePathName, conf);

		// bfFileInputStream = new FSDataInputStream((new
		// Path(bfFile)).getFileSystem(conf).open(new Path(bfFile))); //Opening
		// a hdfs file
		// Opening the local filter file
		FileInputStream fileInputStream = new FileInputStream(localFilePathName);
		bfFileInputStream = new DataInputStream(fileInputStream);

		//// System.out.println("\n >> Load Local Compression Filter File \n" +
		//// localFilePathName + "**\n");

		if (bfFileInputStream != null) {
			this.readFields(bfFileInputStream);
			bfFileInputStream.close();
			fileInputStream.close();
			result = true;
		}

		return result;

	}
	
	/**
	 * Load a Local Filter File into the HB table
	 * 
	 * @param localFilePathName
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public boolean getLocalFilterFile(String localFilePathName, Configuration conf) throws IOException {
		boolean result = false;
		// FSDataInputStream bfFileInputStream=null;
		DataInputStream hbtFileInputStream = null;

		// Opening the local filter file
		FileInputStream fileInputStream = new FileInputStream(localFilePathName);
		hbtFileInputStream = new DataInputStream(fileInputStream);

		if (hbtFileInputStream != null) {
			this.readFields(hbtFileInputStream);
			hbtFileInputStream.close();
			fileInputStream.close();
			result = true;
		}

		return result;

	}
	
	/**
	 * Load a HDFS Compression HB File into the hb To load the
	 * HBT from HDFS Compression File
	 * 
	 * @param HdfsArchive
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public boolean getHDFSCompressionFilterFile(String HdfsArchive, int iteration, Configuration conf)
			throws IOException {
		DataInputStream hbtFileInputStream = getHdfsArchiveInputStream(HdfsArchive, iteration, conf);

		if (hbtFileInputStream != null) {
			// Loading the filter file, dfFile =
			// path_to_Decompressed_Hdfs_Archive_input.txt.filter.tar.gz/input.txt
			this.readFields(hbtFileInputStream); // <--- The Bloom filter size is
												// initialized and copied here
			hbtFileInputStream.close();
		} else {
			return false;
		}

		return true;
	}


	public boolean getHDFSCompressionFilterFile(String HdfsArchive, Configuration conf) throws IOException {
		DataInputStream hbtFileInputStream = getHdfsArchiveInputStream(HdfsArchive, conf);

		if (hbtFileInputStream != null) {
			// Loading the filter file, dfFile =
			// path_to_Decompressed_Hdfs_Archive_input.txt.filter.tar.gz/input.txt
			this.readFields(hbtFileInputStream); // <--- The Bloom filter size is
												// initialized and copied here
			hbtFileInputStream.close();
		} else {
			return false;
		}

		return true;
	}

	/**
	 * Return a FileInputStream of a decompressed local hb file
	 * 
	 * @param HdfsArchive
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public DataInputStream getHdfsArchiveInputStream(String HdfsArchive, int iteration, Configuration conf)
			throws IOException {
		String hbtLocalFile = null;
		String HdfsArchiveName = (new Path(HdfsArchive)).getName();

		// Specifying path_to_local_working_directory/input.txt.filter.gz
		hbtLocalFile = conf.get("mapred.local.dir") + "/"
				+ HdfsArchiveName.substring(0, HdfsArchiveName.lastIndexOf(CONST_FILTERS.compressHFFNameExtension))
				+ "_" + iteration;

		// Decompressing a hdfs compressed filter file
		//// System.out.println("Decompress Hdfs Archive " + HdfsArchive);
		//// System.out.println("Decompress local archive " + bfLocalFile);
		FileDecompressor.decompressHdfsFile(HdfsArchive, hbtLocalFile, conf);

		// FSDataInputStream bfFileInputStream=null;
		DataInputStream dfLocalFileInputStream = null;
		// Opening the local filter file
		dfLocalFileInputStream = new DataInputStream(new FileInputStream(hbtLocalFile));
		return dfLocalFileInputStream;
	}


	public DataInputStream getHdfsArchiveInputStream(String HdfsArchive, Configuration conf) throws IOException {
		String hbtLocalFile = null;
		String HdfsArchiveName = (new Path(HdfsArchive)).getName();

		hbtLocalFile = CONST.HADOOP_TMPDIR + "/"
				+ HdfsArchiveName.substring(0, HdfsArchiveName.lastIndexOf(CONST_FILTERS.compressHFFNameExtension));

		// Decompressing a hdfs compressed filter files
		//// System.out.println("Decompress Hdfs Archive " + HdfsArchive);
		//// System.out.println("Decompress local archive " + bfLocalFile);
		FileDecompressor.decompressHdfsFile(HdfsArchive, hbtLocalFile, conf);

		// FSDataInputStream bfFileInputStream=null;
		DataInputStream dfLocalFileInputStream = null;
		// Opening the local filter file
		dfLocalFileInputStream = new DataInputStream(new FileInputStream(hbtLocalFile));
		return dfLocalFileInputStream;
	}
	
	public void print() {
		System.out.println("hff: ");
		for (int i=0;i<vectorSize;i++) {
			//System.out.println("hb " + i + " " + this.vectorSize);
			System.out.print(i + "\t" + (bitset1.get(i)==true ? 1 : 0) + "\t" + (bitset2.get(i)==true ? 1 : 0) + "\t");
			System.out.println(list[i]);
		}
	}

	public String membershipBinaryTest(String str) {
		try {
			int p = convertB2I(str)% CONST_FILTERS.vectorsize;
			return list[p];
			} catch(NumberFormatException e) {
				System.out.println("Key is not number");
				return null;
			}
	}
	
	public String membershipTest(String str,Boolean R) {
			int p = Math.abs(CONST_FILTERS.hashFunction.hash(str.getBytes())) % CONST_FILTERS.vectorsize;
			if(!bitset1.get(p) || !bitset2.get(p)) return list[p]; //only real balls of elements exist
			BitSet b = convertS2B(list[p]); //else list include 2 list balls of 2 elements 
			if(R) {
					b.and(bitset2);
			} else { //S
					b.and(bitset1);
			}
			/*tmps=b.toString();
			if (tmps.length()>2) return tmps.substring(1,tmps.length()-1);
			else return null;*/
			return convertB2S(b);
	
//			return list[p];
	}
	
	public String membershipTestcalcul(String str,Boolean R) {
		int p = Math.abs(CONST_FILTERS.hashFunction.hash(str.getBytes())) % CONST_FILTERS.vectorsize;
		BitSet b = new BitSet(CONST_FILTERS.vectorsize);
		b = convertS2B(list[p]);
		/*
		 * if (bitset1.get(p)==false) { b.and(bitset1); } else if
		 * (bitset2.get(p)==false) { b.and(bitset2); } else { BitSet tmp = new
		 * BitSet(CONSTANTS_FILTERS.vectorsize); tmp.or(bitset1); tmp.or(bitset2);
		 * b.and(tmp); }
		 */
		if(R) {
				b.and(bitset2);
		} else { //S
				b.and(bitset1);
		}
		/*tmps=b.toString();
		if (tmps.length()>2) return tmps.substring(1,tmps.length()-1);
		else return null;*/
		return convertB2S(b);

//		return list[p];
}
	
	static void permute(char[] arr, int pos, int distance, char[] candidates)
	{
	   if (pos == arr.length)
	   {
	      System.out.println(new String(arr));
	      return;
	   }
	   // distance > 0 means we can change the current character,
	   //   so go through the candidates
	   if (distance > 0)
	   {
	      char temp = arr[pos];
	      for (int i = 0; i < candidates.length; i++)
	      {
	         arr[pos] = candidates[i];
	         int distanceOffset = 0;
	         // different character, thus decrement distance
	         if (temp != arr[pos])
	            distanceOffset = -1;
	         permute(arr, pos+1, distance + distanceOffset, candidates);
	      }
	      arr[pos] = temp;
	   }
	   // otherwise just stick to the same character
	   else
	      permute(arr, pos+1, distance, candidates);
	}
	
	public static void ball(char[] c, int length, char[] alphabet) {
		for(int i = 0; i<length; i++) {
			for(int j = 0; j<alphabet.length; j++) {
				c[i]=alphabet[j];
				System.out.println(String.valueOf(c));
			}
		}
	}
	
	public static String generateBall(String ball,char[] s, char[] ms, int index, int thres,char[] ALPHABET) throws IOException, InterruptedException
    {
		/**
		 * index : length-1
		 * 
		 */
		char[] local = ms.clone();
		if(index > 0 && thres > 0) {
			ball = generateBall(ball, s, ms, index - 1, thres,ALPHABET);
		}
		for(char value : ALPHABET)
	        {
	    	if(value != s[index])
			{
			    local[index] = value;
			    //int ps = Math.abs(CONSTANTS_FILTERS.hashFunction.hash(String.valueOf(local).getBytes()) % CONSTANTS_FILTERS.vectorsize);
			    //String tmpps = String.valueOf(ps);
			    //ball = ball.concat(",").concat(tmpps);
			    ball = ball.concat(",").concat(String.valueOf(local));
			    if(index > 0 && thres > 1) {
			    	ball = generateBall(ball,s, local, index - 1, thres - 1, ALPHABET);
			    }
			}
	        }//end for loop
		return ball;
    }//end generateBall
	
	public static void main(String[] args) throws IOException, InterruptedException {
//		BitSetWritable b3;
		/*		b1=new BitSetWritable(64);
		b2=new BitSetWritable(64);
		
		b1.addStrKey("0000",64);
		b1.addStrKey("1010",64);
		b2.addStrKey("1110",64);
		b2.addStrKey("1000",64);
		
		b1.print(64);
		b2.print(64);
		
*/		
//		b3=new BitSetWritable(CONSTANTS_FILTERS.vectorsize);
//		b3.addBinaryKey("0000");
//		b3.print();
		
//		b1.unionSet(b2);
//		b1.print();
		
//		HammingBallTable2 hb;
//		hb = new HammingBallTable2(CONSTANTS_FILTERS.vectorsize, 1, CONSTANTS_FILTERS.hashType, 4, 1, true);
//		hb.printhblist();
		
//		HIFF2 hff=new HIFF2(b3.get(), hb);
//		System.out.println("vectorsize " + hff.vectorSize);
/*		hff.print();
		
		System.out.println("test 0000 " + hff.membershipBinaryTest("0000"));
		System.out.println("test 0111 " + hff.membershipBinaryTest("0111"));
		System.out.println("test 1011 " + hff.membershipBinaryTest("0010"));
*/		
		/*int p = Math.abs(CONSTANTS_FILTERS.hashFunction.hash("0111".getBytes()) % CONSTANTS_FILTERS.vectorsize);
		System.out.println(p);*/
/*		b3=new BitSetWritable(64);
		b3.addStrKey("0000");
		HammingBallTable2 hb;
		hb = new HammingBallTable2(CONSTANTS_FILTERS.vectorsize, 1, CONSTANTS_FILTERS.hashType, 4, 1, true);
		HFF2 hff=new HFF2(b3.get(), hb);
		System.out.println("test 0000 " + hff.membershipTest("0000"));
		System.out.println("test 0111 " + hff.membershipTest("0111"));
		System.out.println("test 1011 " + hff.membershipTest("0010"));
		
		FileOutputStream fileOut = new FileOutputStream("testfile.txt");
		hff.write(new DataOutputStream(fileOut));
		
		FileInputStream fileIn = new FileInputStream("testfile.txt");
		HFF2 hff2 = new HFF2();
		hff2.readFields(new DataInputStream(fileIn));
		hff2.print();
		*/
//		String[] arr = new String[CONSTANTS_FILTERS.vectorsize];
//		BitSet[] arr = new BitSet[CONSTANTS_FILTERS.vectorsize];
//		BallCalculate(arr, CONSTANTS_FILTERS.vectorsize, 1, CONSTANTS_FILTERS.hashType, 4, 1, true);
/*		for(int i=0;i<arr.length;i++) {
			//System.out.println(arr[i]);
//			arr[i]=new BitSet(CONSTANTS_FILTERS.vectorsize);
			arr[i]=new String();
		}
*/		
		/*ArrayList<String> arr = new ArrayList<>();
		arr.add("0000");
		char[] alphabet = {'0','1'};
		HIFF_FZ.generateSpace(arr, "0000".toCharArray(), "0000".toCharArray(), 3, 4, alphabet);
		for(String str : arr) System.out.println(str);*/
		
		char[] alphabet = {'1','2', '3'};
		//HIFF_FZ.testSpaceCalculate(3, alphabet);
		String ball = "123aaa";
		//System.out.println(generateBall(ball, ball.toCharArray(), ball.toCharArray(), ball.length()-1, 1, alphabet ));
		Random r = new Random();
		System.out.println(Math.abs(r.nextInt())%9);
		/*ball = HIFF_FZ.testgenerateBall(ball, "aaa".toCharArray(), "aaa".toCharArray(), 2, 1, alphabet);
		System.out.println(ball);*/
		//HIFF_FZ.testSpaceCalculate(3, alphabet);
		//permute(ball.toCharArray(), 0, 2, alphabet);
		//ball(ball.toCharArray(),3,alphabet);
		
		/*
		 * BitSet b = new BitSet(5); b.set(3); b.set(1); b.set(3); String str =
		 * b.toString(); System.out.println(str);
		 * System.out.println(str.substring(1,str.length()-1));
		 * 
		 * System.out.println(" azE FEF   FZFZF ".replaceAll(" ", "")); String s1 =
		 * "153"; String s2 = "659"; System.out.println(s1.charAt(0)==s2.charAt(0));
		 * System.out.println(s1.charAt(1)==s2.charAt(1));
		 * System.out.println((int)Math.sqrt((double)CONSTANTS.NUM_EXECUTORS));
		 */
		/*
		 * BitSet[] b = new BitSet[2000000]; for(BitSet bi : b) { bi = new
		 * BitSet(2000000); //for(int i =0; i<2000000; i++) // bi.set(i); }
		 */
		/*
		 * ArrayList[] list = new ArrayList[2000000]; for (ArrayList<Integer> arrayList
		 * : list) { arrayList= new ArrayList<Integer>();
		 * 
		 * }
		 */
		Boolean[][] arr = new Boolean[2000000][2000000]; //outofheap
		
	}
}
