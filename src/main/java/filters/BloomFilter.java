package filters;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 
	@author tttquyen
 *
 */
public class BloomFilter extends Filter implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final byte[] bitvalues = new byte[] { (byte) 0x01, (byte) 0x02, (byte) 0x04, (byte) 0x08,
			(byte) 0x10, (byte) 0x20, (byte) 0x40, (byte) 0x80 };

	/** The bit vector. */
	BitSet bits;

	/** Default constructor - use with readFields */
	public BloomFilter() {
		super();
	}

	/*	*//**
			 * contructor for read field
			 * 
			 * @param size
			 * @author tttquyen
			 */
	public BloomFilter(int size) {
		super();
		vectorSize = size;
	}

	/**
	 * Constructor
	 * 
	 * @param vectorSize
	 *            The vector size of <i>this</i> filter. // Large (Tinh toan sao
	 *            cho it ton bo nho nhat. Demo 150000 bit)
	 * @param nbHash
	 *            The number of hash function to consider. // 8 hash
	 * @param hashType
	 *            type of the hashing function (see
	 *            {@link org.apache.hadoop.util.hash.Hash}).
	 */
	public BloomFilter(int vectorSize, int nbHash, int hashType) {
		super(vectorSize, nbHash, hashType);

		bits = new BitSet(this.vectorSize);
	}

	// Cang's additional function
	public BloomFilter(BloomFilter copy) {
		super(copy.vectorSize, copy.nbHash, copy.hashType);

		bits = new BitSet(copy.vectorSize);
		bits.or(copy.bits);
	}

	// Cang's additional function
	public int getnbHash() {
		return this.nbHash;
	}
	
	public BloomFilter add(BitSet b) {
		if(b!=null && !b.isEmpty()) bits.or(b);
		return this;
	}
	
	public BloomFilter clear() {
		bits.clear();
		return this;
	}
	
	/**
	 * for spark accumulator
	 * @param bf
	 * @return
	 */
	public BloomFilter add(BloomFilter bf) {
		//System.out.println("add Spark function " + this.isEmpty() + " " + bf.isEmpty());
		//this.or(bf);
		bits.or(bf.bits);
		return this;
		
	}
	
	/**
	 * for spark accumulator
	 * @param bf
	 * @return
	 */
	public BloomFilter add(String str) {
		//System.out.println("add Spark function " + this.isEmpty() + " " + bf.isEmpty());
		//this.or(bf);
		int[] h = hash.hash(new Key(str.getBytes()));
		hash.clear();

		for (int i = 0; i < nbHash; i++) {
			bits.set(h[i]);
		}
		return this;
		
	}
	
	public BitSet getBitSet() {
		return bits;
	}

	@Override
	public void add(Key key) {
		// System.out.print("--- Begin add key: " + new String(key.getBytes()));

		if (key == null || key.toString().length() <= 0) {
			throw new NullPointerException("key cannot be null");
		}

		int[] h = hash.hash(key);
		hash.clear();

		for (int i = 0; i < nbHash; i++) {
			bits.set(h[i]);
		}

		// System.out.println(" (OK)---");
	}

	@Override
	public void and(Filter filter) {
		if (filter == null || !(filter instanceof BloomFilter)
		// || filter.vectorSize != this.vectorSize
				|| filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be and-ed");
		}

		this.bits.and(((BloomFilter) filter).bits);
	}

	@Override
	public boolean membershipTest(Key key) {
		if (key == null) {
			throw new NullPointerException("key cannot be null");
		}

		int[] h = hash.hash(key);
		hash.clear();
		for (int i = 0; i < nbHash; i++) {
			if (!bits.get(h[i])) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void not() {
		bits.flip(0, vectorSize - 1);
	}

	@Override
	public void or(Filter filter) {
		if (filter == null || !(filter instanceof BloomFilter)
		// || filter.vectorSize != this.vectorSize
				|| filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be or-ed");
		}
		bits.or(((BloomFilter) filter).bits);
	}

	@Override
	public void xor(Filter filter) {
		if (filter == null || !(filter instanceof BloomFilter)
		// || filter.vectorSize != this.vectorSize
				|| filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be xor-ed");
		}
		bits.xor(((BloomFilter) filter).bits);
	}

	@Override
	public String toString() {
		return bits.toString();
	}

	/**
	 * @return size of the the bloomfilter
	 */
	public int getVectorSize() {
		return this.vectorSize;
	}

	// Writable

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		byte[] bytes = new byte[getNBytes()];
		for (int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
			if (bitIndex == 8) {
				bitIndex = 0;
				byteIndex++;
			}
			if (bitIndex == 0) {
				bytes[byteIndex] = 0;
			}
			if (bits.get(i)) {
				bytes[byteIndex] |= bitvalues[bitIndex];
			}
		}
		out.write(bytes);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		bits = new BitSet(this.vectorSize);
		byte[] bytes = new byte[getNBytes()];
		in.readFully(bytes);
		for (int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
			if (bitIndex == 8) {
				bitIndex = 0;
				byteIndex++;
			}
			if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0) {
				bits.set(i);
			}
		}
	}

	/* @return number of bytes needed to hold bit vector */
	private int getNBytes() {
		return (vectorSize + 7) / 8;
	}

	// Compress and save the difference filter to a file.filter.gz on HDFS
	public void compressAndSaveFilterToHDFS(String hdfsCompressionFilePathName, Configuration conf)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// String hdfsUri = "hdfs://localhost:9000/"; String hdfsUri = "file:/";
		// String hdfsCompressionFilePathName = BLOOMFILTER_PATH + "/" +
		// bloomName + ".filter.gz";
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

	// Load a Local Compression Filter File into the bloom filter
	public boolean getLocalCompressionFilterFile(String localCompressionFilePathName, Configuration conf)
			throws IOException {
		boolean result = false;
		// FSDataInputStream bfFileInputStream=null;
		DataInputStream bfFileInputStream = null;

		// String localFilePathName = localCompressionFilePathName.substring(0,
		// localCompressionFilePathName.lastIndexOf(".")) ;
		/*String localFilePathName = localCompressionFilePathName.substring(0,
				localCompressionFilePathName.lastIndexOf(CONSTANTS_FILTERS.compressNameExtension));*/
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
	 * Load a Local Filter File into the bloom filter
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
		DataInputStream bfFileInputStream = null;

		// Opening the local filter file
		FileInputStream fileInputStream = new FileInputStream(localFilePathName);
		bfFileInputStream = new DataInputStream(fileInputStream);

		if (bfFileInputStream != null) {
			this.readFields(bfFileInputStream);
			bfFileInputStream.close();
			fileInputStream.close();
			result = true;
		}

		return result;

	}

	/**
	 * Load a HDFS Compression Filter File into the bloom filter To load the
	 * Bloom filter from HDFS Compression File
	 * 
	 * @param HdfsArchive
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public boolean getHDFSCompressionFilterFile(String HdfsArchive, int iteration, Configuration conf)
			throws IOException {
		DataInputStream bfFileInputStream = getHdfsArchiveInputStream(HdfsArchive, iteration, conf);

		if (bfFileInputStream != null) {
			// Loading the filter file, dfFile =
			// path_to_Decompressed_Hdfs_Archive_input.txt.filter.tar.gz/input.txt
			this.readFields(bfFileInputStream); // <--- The Bloom filter size is
												// initialized and copied here
			bfFileInputStream.close();
		} else {
			return false;
		}

		return true;
	}

	/**
	 * Load a HDFS Compression Filter File into the bloom filter To load the
	 * Bloom filter from HDFS Compression File
	 * 
	 * @param HdfsArchive
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public boolean getHDFSCompressionFilterFile(String HdfsArchive, String reduceID, int iteration, Configuration conf)
			throws IOException {
		DataInputStream bfFileInputStream = getHdfsArchiveInputStream(HdfsArchive, reduceID, iteration, conf);

		if (bfFileInputStream != null) {
			// Loading the filter file, dfFile =
			// path_to_Decompressed_Hdfs_Archive_input.txt.filter.tar.gz/input.txt
			this.readFields(bfFileInputStream); // <--- The Bloom filter size is
												// initialized and copied here
			bfFileInputStream.close();
		} else {
			return false;
		}

		return true;
	}

	public boolean getHDFSCompressionFilterFile(String HdfsArchive, Configuration conf) throws IOException {
		DataInputStream bfFileInputStream = getHdfsArchiveInputStream(HdfsArchive, conf);

		if (bfFileInputStream != null) {
			// Loading the filter file, dfFile =
			// path_to_Decompressed_Hdfs_Archive_input.txt.filter.tar.gz/input.txt
			this.readFields(bfFileInputStream); // <--- The Bloom filter size is
												// initialized and copied here
			bfFileInputStream.close();
		} else {
			return false;
		}

		return true;
	}

	/**
	 * Return a FileInputStream of a decompressed local filter file
	 * 
	 * @param HdfsArchive
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public DataInputStream getHdfsArchiveInputStream(String HdfsArchive, int iteration, Configuration conf)
			throws IOException {
		String bfLocalFile = null;
		String HdfsArchiveName = (new Path(HdfsArchive)).getName();

		// Specifying path_to_local_working_directory/input.txt.filter.gz
		bfLocalFile = conf.get("mapred.local.dir") + "/"
				+ HdfsArchiveName.substring(0, HdfsArchiveName.lastIndexOf(CONST_FILTERS.compressNameExtension))
				+ "_" + iteration;

		// Decompressing a hdfs compressed filter file
		//// System.out.println("Decompress Hdfs Archive " + HdfsArchive);
		//// System.out.println("Decompress local archive " + bfLocalFile);
		FileDecompressor.decompressHdfsFile(HdfsArchive, bfLocalFile, conf);

		// FSDataInputStream bfFileInputStream=null;
		DataInputStream dfLocalFileInputStream = null;
		// Opening the local filter file
		dfLocalFileInputStream = new DataInputStream(new FileInputStream(bfLocalFile));
		return dfLocalFileInputStream;
	}

	/**
	 * Return a FileInputStream of a decompressed local filter file
	 * 
	 * @param HdfsArchive
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public DataInputStream getHdfsArchiveInputStream(String HdfsArchive, String reduceID, int iteration,
			Configuration conf) throws IOException {
		String bfLocalFile = null;
		String HdfsArchiveName = (new Path(HdfsArchive)).getName();

		// Specifying path_to_local_working_directory/input.txt.filter.gz
		bfLocalFile = conf.get("mapred.local.dir") + "/"
				+ HdfsArchiveName.substring(0, HdfsArchiveName.lastIndexOf(CONST_FILTERS.compressNameExtension))
				+ "_" + iteration + "_" + reduceID;

		// Decompressing a hdfs compressed filter file
		//// System.out.println("Decompress Hdfs Archive " + HdfsArchive);
		//// System.out.println("Decompress local archive " + bfLocalFile);
		FileDecompressor.decompressHdfsFile(HdfsArchive, bfLocalFile, conf);

		// FSDataInputStream bfFileInputStream=null;
		DataInputStream dfLocalFileInputStream = null;
		// Opening the local filter file
		dfLocalFileInputStream = new DataInputStream(new FileInputStream(bfLocalFile));
		return dfLocalFileInputStream;
	}

	public DataInputStream getHdfsArchiveInputStream(String HdfsArchive, Configuration conf) throws IOException {
		String bfLocalFile = null;
		String HdfsArchiveName = (new Path(HdfsArchive)).getName();

		// Specifying path_to_local_working_directory/input.txt.filter.tar.gz
		// bfLocalFile = conf.get("mapred.local.dir") + "/"
		// + HdfsArchiveName.substring(0,
		// HdfsArchiveName.lastIndexOf(CONSTANTS_FILTERS.compressNameExtension));

		bfLocalFile = CONST.HADOOP_TMPDIR + "/"
				+ HdfsArchiveName.substring(0, HdfsArchiveName.lastIndexOf(CONST_FILTERS.compressNameExtension));

		// Decompressing a hdfs compressed filter files
		//// System.out.println("Decompress Hdfs Archive " + HdfsArchive);
		//// System.out.println("Decompress local archive " + bfLocalFile);
		FileDecompressor.decompressHdfsFile(HdfsArchive, bfLocalFile, conf);

		// FSDataInputStream bfFileInputStream=null;
		DataInputStream dfLocalFileInputStream = null;
		// Opening the local filter file
		dfLocalFileInputStream = new DataInputStream(new FileInputStream(bfLocalFile));
		return dfLocalFileInputStream;
	}

	/**
	 * New Add function Create a new genaral filter by union all filter parts in
	 * FILTER_INPUT_HDFS_DIRECTORY in HDFS
	 * 
	 * @param FILTER_INPUT_HDFS_DIRECTORY
	 * @param UNIONFILTER_OUTPUT_HDFS_FILE
	 * @param conf
	 * @return union BF
	 * @throws IOException
	 * @author tttquyen
	 */
	// @SuppressWarnings("deprecation")
	public static BloomFilter generateHdfsUnionFilter(String FILTER_INPUT_HDFS_DIRECTORY,
			String UNIONFILTER_OUTPUT_HDFS_FILE, int iteration, Configuration conf) throws IOException {
		// for temp filter and union filter
		BloomFilter bf = new BloomFilter(), unionBF = null;

		// Specifying the output directory @ runtime
		Path filtersPath = new Path(FILTER_INPUT_HDFS_DIRECTORY);
		FileSystem fs = FileSystem.get(conf);

		PathFilter fileFilter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().endsWith(CONST_FILTERS.compressNameExtension);
			}
		};

		// Only read filter files with extension .filter.gz
		FileStatus[] status = fs.listStatus(filtersPath, fileFilter);

		for (int i = 0; i < status.length; i++) {
			bf.getHDFSCompressionFilterFile(status[i].getPath().toString(), iteration, conf);
			if (unionBF == null) {
				unionBF = new BloomFilter(bf);
				// unionBF = bf;
			} else {
				unionBF.or(bf);
			}
		}

		// *Saving union filter in bfs
		// Creating a HDFS compression DataOutputStream associated with a
		// (hdfs_file.codec, conf)
		// if(unionBF!=null && !unionBF.isEmpty()) {
		FileCompressor fileCompressor = new FileCompressor();
		DataOutputStream hdfsCompOutStream = fileCompressor.createHdfsCompOutStream(UNIONFILTER_OUTPUT_HDFS_FILE, conf);

		// Saving the union bloom filter to the HDFS compression
		// DataOutputStream
		unionBF.write(hdfsCompOutStream);

		// Closing the HDFS compression DataOutputStream
		fileCompressor.close();
		hdfsCompOutStream.close();

		// fs.delete(new Path(FILTER_INPUT_HDFS_DIRECTORY)); //Not delete for
		// test
		// }
		return unionBF;

	}

	/**
	 * Check filter is empty or not
	 * 
	 * @return
	 * @author tttquyen
	 */
	public boolean isEmpty() {
		return bits.isEmpty();
	}

	// public static void main(String args[]) {
	// BloomFilter bf1 = new BloomFilter(100, 8, Hash.MURMUR_HASH);
	// BloomFilter bf2 = new BloomFilter(100, 8, Hash.MURMUR_HASH);
	//
	// Key key1 = new Key("a".getBytes());
	// Key key2 = new Key("b".getBytes());
	//
	// bf1.add(key1);
	// bf2.add(key2);
	//
	// //System.out.println("Test bf1 " + bf1.membershipTest(key1) +
	// bf1.membershipTest(key2));
	// //System.out.println("Test bf2 " + bf2.membershipTest(key1) +
	// bf2.membershipTest(key2));
	//
	// bf1.or(bf2);
	// //System.out.println("Test bf1 " + bf1.membershipTest(key1) +
	// bf1.membershipTest(key2));
	//
	// }

}// end class
