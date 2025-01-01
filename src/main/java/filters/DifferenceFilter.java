package filters;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;

/**
 * Implements a <i>Difference Filter with Lossy Hash Table bucket=64 bits of 1/2 md5 (long = 16 characters)</i>, as defined by PHAN Thuong Cang
 */

/**
 * Author: ptcang 
 * Modify by Quyen
 * @author tttquyen
 *
 */

public class DifferenceFilter implements Writable  {

	DynamicBloomFilter  dbf;
	LossyHashFilter lhf;


	public DifferenceFilter(){}


	/**
	 * Constructor
	 * @param vectorSize The vector size of <i>this</i> filter.
	 * @param nbHash The number of hash function to consider.
	 * @param hashType type of the hashing function.
	 * @param nr the maximum number of keys to record in a standard Bloom filter row.
	 */
	public DifferenceFilter(int vectorSizeDBF, int nbHashDBF, int hashTypeDBF, int nr, int vectorSizeLHF, int nbHashLHF, int hashTypeLHF ) {
		dbf = new DynamicBloomFilter(vectorSizeDBF, nbHashDBF, hashTypeDBF, nr); // nr the maximum number of key per row
		lhf = new LossyHashFilter(vectorSizeLHF, nbHashLHF, hashTypeLHF);  // the maximum number of buckets, the number of Hash functions, the type of hash function
	}
	
	public DifferenceFilter(DynamicBloomFilter newdbf, LossyHashFilter newlhf) {
		dbf = newdbf;
		lhf = newlhf;
	}
	
	//Quyen's add function
	public DifferenceFilter(DifferenceFilter copy) {
		dbf = new DynamicBloomFilter(copy.getDBF());
		lhf = new LossyHashFilter(copy.getLHF());
	}
	
	public DynamicBloomFilter getDBF() {
		return dbf;
	}
	
	public LossyHashFilter getLHF() {
		return lhf;
	}

	public void add(Key key) {
		dbf.add(key);
		lhf.add(key);
	}


	public void and(DifferenceFilter filter) {
		if (filter == null){
			throw new IllegalArgumentException("filters cannot be or-ed");
		}

		this.dbf.and(filter.dbf);
		this.lhf.and(filter.lhf);
	}

	/*
	 * Query a key x into the difference filter 
	 * return 0: is not a difference, 1: is a difference, 2: unknown
	 * after querying, the key x is inserted into the filter
	 */
	public int membershipTest(Key key) {
		int answer = 2; // 

		if ( ! dbf.membershipTest(key)) {
			answer = 1; // "yes", key is NOT in DBF and is a difference element
		}
		else 
			if(lhf.membershipTest(key))
				answer = 0; // "no", key is in both DBF and LHF, and is NOT a difference element
			else
				answer = 2; // "unknown", key is an "unknown" element
		if(answer != 0) {
			lhf.add(key);
			if(answer == 1)
				dbf.add(key);		
		}

		return answer;
	}

	public void or(DifferenceFilter filter) {
		if (filter == null){
			throw new IllegalArgumentException("filters cannot be or-ed");
		}

		this.dbf.or(filter.dbf);
		this.lhf.or(filter.lhf);
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();

		res.append(dbf.toString());
		res.append(Character.LINE_SEPARATOR);
		res.append(lhf.toString());

		return res.toString();
	}



	// Writable


	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		dbf.write(out);
		lhf.write(out);
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		dbf = new DynamicBloomFilter();
		lhf = new LossyHashFilter();
		//dbf = new DynamicBloomFilter(CONSTANTS_FILTERS.SizeBF);
		//lhf = new LossyHashFilter(CONSTANTS_FILTERS.numBucketLHT);
		dbf.readFields(in);
		lhf.readFields(in);
	}


	//Compress and save the difference filter to a file.bloom.gz on HDFS
	public void compressAndSaveFilterToHDFS(String hdfsCompressionFilePathName, Configuration conf) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//String hdfsUri = "hdfs://localhost:9000/"; String hdfsUri = "file:/";		
		//String hdfsCompressionFilePathName = BLOOMFILTER_PATH + "/" + bloomName + ".bloom.gz";
//		System.out.println("\n >> compressAndSaveFilter(), Hdfs compressioned filter file=" + hdfsCompressionFilePathName);

		//Creating a HDFS compression DataOutputStream associated with a (hdfs_file.codec, conf)
		FileCompressor fileCompressor = new FileCompressor();
		DataOutputStream hdfsCompOutStream = fileCompressor.createHdfsCompOutStream(hdfsCompressionFilePathName, conf);

		//Saving the bloom filter to the HDFS compression DataOutputStream
		this.write(hdfsCompOutStream);

		//Closing the HDFS compression DataOutputStream
		fileCompressor.close();
		hdfsCompOutStream.close();
	}


	//Load a Local Compression Filter File into the differnce filter
	public boolean getLocalCompressionFilterFile(String localCompressionFilePathName, Configuration conf) throws IOException{
		boolean result=false;
		//FSDataInputStream bfFileInputStream=null;
		DataInputStream bfFileInputStream = null;

		String localFilePathName = localCompressionFilePathName.substring(0, localCompressionFilePathName.lastIndexOf(".")) ;

		//Uncompress a local compressed filter file
		FileDecompressor.decompressLocalFile(localCompressionFilePathName, localFilePathName, conf) ;

		//bfFileInputStream = new FSDataInputStream((new Path(bfFile)).getFileSystem(conf).open(new Path(bfFile))); //Opening a hdfs file
		//Opening the local filter file
		FileInputStream fileInputStream = new FileInputStream(localFilePathName);
		bfFileInputStream =  new  DataInputStream (fileInputStream);

		//System.out.println("\n   >> Load Local Compression Filter File \n" + localFilePathName + "**\n");

		if(bfFileInputStream != null){
			this.readFields(bfFileInputStream);		
			bfFileInputStream.close();
			fileInputStream.close();
			result = true;
		}

		return result;		

	}

	/**
	 * Load a Local Filter File into the difference filter
	 * @param localFilePathName
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public boolean getLocalFilterFile(String localFilePathName, Configuration conf) throws IOException{
		boolean result=false;
		//FSDataInputStream bfFileInputStream=null;
		DataInputStream dfFileInputStream = null;
		//Opening the local filter file
		FileInputStream fileInputStream = new FileInputStream(localFilePathName);
		dfFileInputStream =  new  DataInputStream (fileInputStream);
		//System.out.println("\n   >> Load Local Compression Filter File \n" + localFilePathName + "**\n");

		if(dfFileInputStream != null){
			this.readFields(dfFileInputStream);		
			dfFileInputStream.close();
			fileInputStream.close();
			result = true;
		}
		
		return result;		

	}
	
	/**
	 * Load a HDFS Compression Filter File into the difference filter
	*  To load the Difference filter from HDFS Compression File 
	 * @param HdfsArchive
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public boolean getHDFSCompressionFilterFile(String HdfsArchive, int iteration, Configuration conf) throws IOException{
//		System.out.println("\n******Bat dau goi ham getHDFSCompression.getHdfsArchiveInputStream...");
		DataInputStream  dfFileInputStream = getHdfsArchiveInputStream (HdfsArchive, iteration, conf); 
//		System.out.println("\n******Ket thuc ham getHDFSArchiveInputStream");
		if(dfFileInputStream != null){
			//Loading the filter file, dfFile = path_to_Decompressed_Hdfs_Archive_input.txt.filter.tar.gz/input.txt
			this.readFields(dfFileInputStream); //<--- The Different filter size is initialized and copied here
			dfFileInputStream.close();
		}
		else {
			System.out.println("Can not read dfInputStream");
			return false;
		}

		return true;
	}

	/**
	 * Return a FileInputStream of a decompressed local filter file
	 * @param HdfsArchive
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public DataInputStream  getHdfsArchiveInputStream (String HdfsArchive, int iteration, Configuration conf) throws IOException{
		String dfLocalFile = null;
		String HdfsArchiveName = (new Path(HdfsArchive)).getName();

		//Specifying path_to_local_working_directory/DF_DeltaF0000_0.filter.gz --> localdir/DF_DeltaF0000_0_<itearation>
		dfLocalFile = conf.get("mapred.local.dir") + "/" + HdfsArchiveName.substring(0, HdfsArchiveName.lastIndexOf(CONST_FILTERS.compressNameExtension)) + "_" + iteration ;
//		System.out.println("\n**********Bat dau goi ham getHDFSArchive.Decompress");
		//Decompressing a hdfs compressed filter file
		FileDecompressor.decompressHdfsFile(HdfsArchive, dfLocalFile, conf) ;
//		System.out.println("\n+++++++++++Tao ra file cuc bo  " + dfLocalFile);
//		System.out.println("\n**********Ket thuc ham Decompress");
		//FSDataInputStream bfFileInputStream=null;
		DataInputStream dfLocalFileInputStream = null;
		//Opening the local filter file
		dfLocalFileInputStream =  new  DataInputStream (new FileInputStream(dfLocalFile));
		return dfLocalFileInputStream;
	}

	
	/**
	 * Load a HDFS Compression Filter File into the difference filter for special reduce
	*  To load the Difference filter from HDFS Compression File 
	 * @param HdfsArchive
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public boolean getHDFSCompressionFilterFile(String HdfsArchive, String reduceID, int iteration, Configuration conf) throws IOException{
		//System.out.println("\n******Bat dau goi ham getHDFSCompression.getHdfsArchiveInputStream...");
		DataInputStream  dfFileInputStream = getHdfsArchiveInputStream (HdfsArchive, reduceID ,iteration, conf); 
		//System.out.println("\n******Ket thuc ham getHDFSArchiveInputStream");
		if(dfFileInputStream != null){
			//Loading the filter file, dfFile = path_to_Decompressed_Hdfs_Archive_input.txt.filter.tar.gz/input.txt
			this.readFields(dfFileInputStream); //<--- The Different filter size is initialized and copied here
			dfFileInputStream.close();
		}
		else {
			System.out.println("Can not read dfInputStream");
			return false;
		}

		return true;
	}

	/**
	 * Return a FileInputStream of a decompressed local filter file for special reduce
	 * @param HdfsArchive
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public DataInputStream  getHdfsArchiveInputStream (String HdfsArchive, String reduceID ,int iteration, Configuration conf) throws IOException{
		String dfLocalFile = null;
		String HdfsArchiveName = (new Path(HdfsArchive)).getName();

		//Specifying path_to_local_working_directory/DF_DeltaF0000_0.filter.gz --> localdir/DF_DeltaF0000_0_<itearation>
		dfLocalFile = conf.get("mapred.local.dir") + "/" + HdfsArchiveName.substring(0, HdfsArchiveName.lastIndexOf(CONST_FILTERS.compressNameExtension)) + "_" + iteration + "_" + reduceID ;
//		System.out.println("\n**********Bat dau goi ham getHDFSArchive.Decompress");
		//Decompressing a hdfs compressed filter file
		FileDecompressor.decompressHdfsFile(HdfsArchive, dfLocalFile, conf) ;
//		System.out.println("\n+++++++++++Tao ra file cuc bo  " + dfLocalFile);
//		System.out.println("\n**********Ket thuc ham Decompress");
		//FSDataInputStream bfFileInputStream=null;
		DataInputStream dfLocalFileInputStream = null;
		//Opening the local filter file
		dfLocalFileInputStream =  new  DataInputStream (new FileInputStream(dfLocalFile));
		return dfLocalFileInputStream;
	}

	
	/**
	 * New Add function
	 * Create a new genaral filter by union all filter parts in FILTER_INPUT_HDFS_DIRECTORY in HDFS
	 * @param FILTER_INPUT_HDFS_DIRECTORY
	 * @param UNIONFILTER_OUTPUT_HDFS_FILE
	 * @param conf
	 * @return union BF
	 * @throws IOException
	 * @author tttquyen
	 */
//	@SuppressWarnings("deprecation")
	public static DifferenceFilter generateHdfsUnionFilter(String FILTER_INPUT_HDFS_DIRECTORY, String UNIONFILTER_OUTPUT_HDFS_FILE, int iteration, Configuration conf) throws IOException{
		DifferenceFilter  unionDF = new DifferenceFilter();
		DifferenceFilter df = new DifferenceFilter();

		//Specifying the output directory @ runtime
		Path filtersPath = new Path(FILTER_INPUT_HDFS_DIRECTORY);
		FileSystem fs = FileSystem.get(conf);

		PathFilter fileFilter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().endsWith(CONST_FILTERS.compressNameExtension);
			}
		};

		//Only read filter files with extension .filter.tar.gz
		FileStatus[] status = fs.listStatus(filtersPath, fileFilter);

		if (status.length > 0) {
			unionDF.getHDFSCompressionFilterFile(status[0].getPath().toString(), iteration, conf);
			for (int i=1;i<status.length;i++){
//				System.out.println("\n*********Dang doc bo loc thu" + i + " : " + status[i].getPath().toString());
				
				df.getHDFSCompressionFilterFile(status[i].getPath().toString(), iteration, conf);
				/*if (df.isEmpty()) {
					System.out.println("\n+++df is empty");
				}
				else {
					System.out.println("\n+++df is not empty, length " + df.getLengthDBF());
				}
				if (unionDF == null) {
					System.out.println("\n+++Doc union dau tien (null) ");
					unionDF = new DifferenceFilter(df);
					//unionDF = df;
				}
				else {*/
					unionDF.or(df);
			/*		System.out.println("\n+++Length of Union " + unionDF.getLengthDBF());
				}*/
			}
		}
		if(unionDF!=null && !unionDF.isEmpty()) {
			//*Saving union filter in dfs 
			//Creating a HDFS compression DataOutputStream associated with a (hdfs_file.codec, conf)		
				FileCompressor fileCompressor = new FileCompressor();
				DataOutputStream hdfsCompOutStream = fileCompressor.createHdfsCompOutStream(UNIONFILTER_OUTPUT_HDFS_FILE, conf);
				if (hdfsCompOutStream == null) {
					System.out.println("\n++++++++Khong tim thay  " + UNIONFILTER_OUTPUT_HDFS_FILE);
				} else {
					//Saving the union bloom filter to the HDFS compression DataOutputStream
//					System.out.println("\n++++++++Bat dau viet Union DF len HDFS ");
					unionDF.write(hdfsCompOutStream);
//					System.out.println("\n++++++++Da viet bo loc Union DF len HDFS ");
					//Closing the HDFS compression DataOutputStream
					fileCompressor.close();
					hdfsCompOutStream.close();		
				}
	//			fs.delete(new Path(FILTER_INPUT_HDFS_DIRECTORY));
		}
		return unionDF;

	}
	
	/**
	 * New Add function
	 * Create a new genaral filter by union all filter parts in FILTER_INPUT_HDFS_DIRECTORY in HDFS
	 * @param FILTER_INPUT_HDFS_DIRECTORY
	 * @param UNIONFILTER_OUTPUT_HDFS_FILE
	 * @param conf
	 * @return union BF
	 * @throws IOException
	 * @author tttquyen
	 */

	public static DifferenceFilter generateHdfsUnionFilter(String previous_DF, String FILTER_INPUT_HDFS_DIRECTORY, String UNIONFILTER_OUTPUT_HDFS_FILE, int iteration, Configuration conf) throws IOException{
		DifferenceFilter  unionDF = new DifferenceFilter();
		DifferenceFilter df = new DifferenceFilter();

		//Specifying the output directory @ runtime
		Path filtersPath = new Path(FILTER_INPUT_HDFS_DIRECTORY);
		FileSystem fs = FileSystem.get(conf);

		PathFilter fileFilter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().endsWith(CONST_FILTERS.compressNameExtension);
			}
		};

		//Only read filter files with extension .filter.tar.gz
		FileStatus[] status = fs.listStatus(filtersPath, fileFilter);

		unionDF.getHDFSCompressionFilterFile(previous_DF, iteration, conf);
//		System.out.println("\n*********Da doc bo loc previous " + " : " + previous_DF);
		
		for (int i=0;i<status.length;i++){
//			System.out.println("\n*********Dang doc bo loc thu" + i + " : " + status[i].getPath().toString());
			
			df.getHDFSCompressionFilterFile(status[i].getPath().toString(), iteration, conf);
//			if (df.isEmpty()) {
//				System.out.println("\n+++df is empty");
//			}
//			else {
//				System.out.println("\n+++df is not empty, length " + df.getLengthDBF());
//			}
			if (unionDF == null) {
//				System.out.println("\n+++Doc union dau tien (null) ");
				unionDF = new DifferenceFilter(df);
				//unionDF = df;
			}
			else {
				unionDF.or(df);
//			System.out.println("\n+++Length of Union " + unionDF.getLengthDBF());
			}
		}
		
		if(unionDF!=null && !unionDF.isEmpty()) {
			//*Saving union filter in dfs 
			//Creating a HDFS compression DataOutputStream associated with a (hdfs_file.codec, conf)		
				FileCompressor fileCompressor = new FileCompressor();
				DataOutputStream hdfsCompOutStream = fileCompressor.createHdfsCompOutStream(UNIONFILTER_OUTPUT_HDFS_FILE, conf);
				if (hdfsCompOutStream == null) {
					System.out.println("\n++++++++Khong tim thay  " + UNIONFILTER_OUTPUT_HDFS_FILE);
				} else {
					//Saving the union bloom filter to the HDFS compression DataOutputStream
//					System.out.println("\n++++++++Bat dau viet Union DF len HDFS ");
					unionDF.write(hdfsCompOutStream);
//					System.out.println("\n++++++++Da viet bo loc Union DF len HDFS ");
					//Closing the HDFS compression DataOutputStream
					hdfsCompOutStream.close();
					fileCompressor.close();
				}
	//			fs.delete(new Path(FILTER_INPUT_HDFS_DIRECTORY));
		}
		return unionDF;

	}
	
	public static DifferenceFilter generateHdfsUnionFilter(String previous_DF, String FILTER_INPUT_HDFS_DIRECTORY, String UNIONFILTER_OUTPUT_HDFS_FILE, int iteration, Configuration conf, StringBuilder message) throws IOException{
		DifferenceFilter  unionDF = new DifferenceFilter();
		DifferenceFilter df = new DifferenceFilter();

		//Specifying the output directory @ runtime
		Path filtersPath = new Path(FILTER_INPUT_HDFS_DIRECTORY);
		FileSystem fs = FileSystem.get(conf);

		PathFilter fileFilter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().endsWith(CONST_FILTERS.compressNameExtension);
			}
		};

		//Only read filter files with extension .filter.tar.gz
		FileStatus[] status = fs.listStatus(filtersPath, fileFilter);

		unionDF.getHDFSCompressionFilterFile(previous_DF, iteration, conf);
		message.append("\n*********Da doc bo loc previous ").append(" : ").append(previous_DF);
		System.out.println("\n*********Da doc bo loc previous " + " : " + previous_DF);
		
		for (int i=0;i<status.length;i++){
			System.out.println("\n*********Dang doc bo loc thu" + i + " : " + status[i].getPath().toString());
			message.append("\n*********Dang doc bo loc thu").append(i).append(" : ").append(status[i].getPath().toString());
			
			df.getHDFSCompressionFilterFile(status[i].getPath().toString(), iteration, conf);
			if (df.isEmpty()) {
				message.append("\n+++df is empty");
				System.out.println("\n+++df is empty");
			}
			else {
				message.append("\n+++df is not empty, length ").append(df.getLengthDBF());
				System.out.println("\n+++df is not empty, length " + df.getLengthDBF());
			}
			if (unionDF == null) {
				message.append("\n+++Doc union dau tien (null) ");
				System.out.println("\n+++Doc union dau tien (null) ");
				unionDF = new DifferenceFilter(df);
//				unionDF = df;
			}
			else {
				message.append("\n+++OR df ");
				unionDF.or(df);
				message.append("\n+++Length of Union ").append(unionDF.getLengthDBF());
			System.out.println("\n+++Length of Union " + unionDF.getLengthDBF());
			}
		}
		
		if(unionDF!=null && !unionDF.isEmpty()) {
			//*Saving union filter in dfs 
			//Creating a HDFS compression DataOutputStream associated with a (hdfs_file.codec, conf)		
				FileCompressor fileCompressor = new FileCompressor();
				DataOutputStream hdfsCompOutStream = fileCompressor.createHdfsCompOutStream(UNIONFILTER_OUTPUT_HDFS_FILE, conf);
				if (hdfsCompOutStream == null) {
					System.out.println("\n++++++++Khong tim thay  " + UNIONFILTER_OUTPUT_HDFS_FILE);
				} else {
					//Saving the union bloom filter to the HDFS compression DataOutputStream
					message.append("\n++++++++Bat dau viet Union DF len HDFS ");
					System.out.println("\n++++++++Bat dau viet Union DF len HDFS ");
					unionDF.write(hdfsCompOutStream);
					message.append("\n++++++++Da viet bo loc Union DF len HDFS ");
					System.out.println("\n++++++++Da viet bo loc Union DF len HDFS ");
					//Closing the HDFS compression DataOutputStream
					hdfsCompOutStream.close();
					fileCompressor.close();
				}
	//			fs.delete(new Path(FILTER_INPUT_HDFS_DIRECTORY));
		}
		return unionDF;

	}
	
	/**
	 * Check filter is empty or not
	 * @return
	 * @author tttquyen
	 */
	public boolean isEmpty() {
		if (dbf.isEmpty()) {
			return true;
		}
		else {
			return false;
		}
	}
	
	/**
	 * 
	 * @return length of DBF
	 * @author tttquyen
	 */
	public int getLengthDBF() {
		return (dbf==null ? -1 : dbf.getLength());
	}
	
	/**
	 * Cut out from begin to position of DBF
	 * Use to OR Function for trust difference DBF
	 * @param position
	 * @author tttquyen
	 */
	public void cutDBFFromPosition(int position) {
		dbf.cutElementsFromPosition(position);
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// @params: (int vectorSizeDBF, int nbHashDBF, int hashTypeDBF, int n0, int vectorSizeLHF, int nbHashLHF, int hashTypeLHF );
		DifferenceFilter df = new DifferenceFilter(30, 3, Hash.MURMUR_HASH, 10 ,    10, 1, Hash.MURMUR_HASH);

		String key1 = "Laurent Cang";
		String key2 = "Quyen Thuat";
		String key3 = "Philippe";


		df.add(new Key(key1.getBytes()));		
		df.add(new Key(key2.getBytes()));

		int result2 = df.membershipTest(new Key(key2.getBytes())); // return 0: is not a difference, 1: is a difference, 2: unknown
		int result3 = df.membershipTest(new Key(key3.getBytes())); // return 0: is not a difference, 1: is a difference, 2: unknown

		System.out.println("\n ** >> Difference Filter: key2=" + key2 + ", kq=" + result2 + " ; and  key3=" + key3 + ", kq=" + result3);

		System.out.println("\n ^^ The content of the difference filter \n df=" + df.toString());			


		////  Save DF to HDFS and Load Filter from Local  ////
		Configuration  conf = new Configuration();
		String URI = new String("hdfs://tttquyen-K46CA:9000/");
		String compressionFilePathName = new String("test.bloom.gz");

		try {
			df.compressAndSaveFilterToHDFS( URI +  compressionFilePathName, conf); // This parameter is HDFS:  URI + FilePathName
		} catch (IOException | InterruptedException e) { e.printStackTrace(); }


		DifferenceFilter  df2 = new DifferenceFilter() ; // No parameters here, parameters will be load from file
		try {
			df2.getLocalCompressionFilterFile(compressionFilePathName, conf); // This parameter is LOCAL:  FilePathName
		} catch (IOException e) { e.printStackTrace(); }

		System.out.println("\n ^^ The content of the loaded difference filter \n df2=" + df2.toString());	

		result2 = df2.membershipTest(new Key(key2.getBytes())); // return 0: is not a difference, 1: is a difference, 2: unknown. The key2 exists in the difference filter
		result3 = df2.membershipTest(new Key(key3.getBytes())); // return 0: is not a difference, 1: is a difference, 2: unknown. After querying, the key3 is inserted into the filter

		System.out.println("\n ** >> Difference Filter: key2=" + key2 + ", kq=" + result2 + " ; and  key3=" + key3 + ", kq=" + result3);

		String key4= "Barrar";
		int result4 = df2.membershipTest(new Key(key4.getBytes())); // return 0: is not a difference, 1: is a difference, 2: unknown		
		System.out.println("\n ** >> Difference Filter: key4=" + key4 + ", kq=" + result4 );
		System.out.println("\n ^^ The content of the difference filter df2 updated after query key4="+ key4 + ",  \n df2=" + df2.toString());			

	}

}//end class
