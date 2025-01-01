package filters;


import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

public class FileCompressor {
	//Variables only use for createHdfsCompOutStream()
	FSDataOutputStream fsDataOutputStream = null;
	CompressionOutputStream compressionOutputStream = null;
	DataOutputStream hdfsCompOutStream = null;

	//Creating a HDFS compression DataOutputStream associated with a (hdfs_file.codec, conf)
	@SuppressWarnings("deprecation")
	public DataOutputStream createHdfsCompOutStream(String hdfsCompressionFileName, Configuration conf){
		//String hdfsCompressionFileName = "/data/output/bloom/UserDetails.txt.bloom.gz";
		//String hdfsUri = "hdfs://localhost:9000/"; //String hdfsUri = "file:/";

		FileSystem fileSystem = null;
		CompressionCodec compressionCodec = null;
		CompressionCodecFactory compressionCodecFactory = null;

		try {
			Path compressionFilePath = new Path(hdfsCompressionFileName);

			//Creating DataOutputStream associated with a file compressionFilePath
			//fileSystem = FileSystem.get(new URI(hdfsUri), conf);
			fileSystem = FileSystem.get(conf);
			fileSystem.delete(compressionFilePath);
			fsDataOutputStream = fileSystem.create(compressionFilePath);

			/* Getting CompressionCodec instance associated with the file extension */
			compressionCodecFactory = new CompressionCodecFactory(conf);			
			compressionCodec = compressionCodecFactory.getCodec(compressionFilePath);

			//Creating compression Output Stream associated with File DataOutputStream
			compressionOutputStream = compressionCodec.createOutputStream(fsDataOutputStream);	

			//Creating DataOutputStream associated with a Compression Output Stream (file, codec)
			hdfsCompOutStream = new DataOutputStream(compressionOutputStream);

		}catch(IOException e2) {
			System.out.println("\nXXXXXXXXXXXXXXXXXXuat hien ngoai le trong ham create HDFS Compress OuputStream:" + e2);
			e2.printStackTrace();
		}

		return hdfsCompOutStream;		
	}

	//Closing all opened output streams
	public void close(){
		try {
			//It should follow the following order
			if( hdfsCompOutStream != null) {
				hdfsCompOutStream.close();
			}

			if( compressionOutputStream != null) {
				compressionOutputStream.close();
			}

			if( fsDataOutputStream != null) {
				fsDataOutputStream.close();
			}			

		}catch(IOException e1){
			e1.printStackTrace();
		}
	}

	//Compressing a local source file into a hdfs destination file
	public static void compressFile(String localSourceFileName, String hdfsCompressionFileName, Configuration conf) {
		//String localSourceFileName = "/home/ptcang/data/output/bloom/UserDetails.txt.bloom";
		//String hdfsCompressionFileName = "/data/output/bloom/UserDetails.txt.bloom.gz";

		//String hdfsUri = "hdfs://localhost:9000/"; //String hdfsUri = "file:/";

		FileSystem fileSystem = null;
		CompressionCodec compressionCodec = null;
		CompressionCodecFactory compressionCodecFactory = null;

		FileInputStream localFileInputStream = null;
		CompressionOutputStream compressionOutputStream = null;

		try {

			//fileSystem = FileSystem.get(new URI(hdfsUri), configuration);
			fileSystem = FileSystem.get(conf);

			Path compressionFilePath = new Path(hdfsCompressionFileName);
			FSDataOutputStream fsDataOutputStream = fileSystem.create(compressionFilePath);

			compressionCodecFactory = new CompressionCodecFactory(conf);
			/* get CompressionCodec instance associated with the file extension */
			compressionCodec = compressionCodecFactory.getCodec(compressionFilePath);
			compressionOutputStream = compressionCodec.createOutputStream(fsDataOutputStream);

			localFileInputStream = new FileInputStream(localSourceFileName);
			IOUtils.copyBytes(localFileInputStream, compressionOutputStream, conf, false);

		}catch(IOException e2) {
			e2.printStackTrace();
		}finally {
			try {
				if( compressionOutputStream != null) {
					compressionOutputStream.close();
				}
				if( localFileInputStream != null) {
					localFileInputStream.close();
				}
			}catch(IOException e1){
				e1.printStackTrace();
			}
		}

		//System.out.println("Successfull Compression");
	}

	//Compressing a byte array into a hdfs destination file
	@SuppressWarnings("deprecation")
	public static void compressBytes(byte[] byteArray, String hdfsCompressionFileName, Configuration conf) {
		//String hdfsCompressionFileName = "/data/output/bloom/UserDetails.txt.bloom.gz";

		//String hdfsUri = "hdfs://localhost:9000/"; //String hdfsUri = "file:/";

		FileSystem fileSystem = null;
		CompressionCodec compressionCodec = null;
		CompressionCodecFactory compressionCodecFactory = null;

		InputStream localFileInputStream = null;
		CompressionOutputStream compressionOutputStream = null;

		try {

			//fileSystem = FileSystem.get(new URI(hdfsUri), conf);
			fileSystem = FileSystem.get(conf);

			Path compressionFilePath = new Path(hdfsCompressionFileName);
			FileSystem.get(conf).delete(compressionFilePath);
			FSDataOutputStream fsDataOutputStream = fileSystem.create(compressionFilePath);

			compressionCodecFactory = new CompressionCodecFactory(conf);
			/* get CompressionCodec instance associated with the file extension */
			compressionCodec = compressionCodecFactory.getCodec(compressionFilePath);
			compressionOutputStream = compressionCodec.createOutputStream(fsDataOutputStream);

			localFileInputStream = new ByteArrayInputStream(byteArray);
			IOUtils.copyBytes(localFileInputStream, compressionOutputStream, conf, false);

		}catch(IOException e2) {
			e2.printStackTrace();
		}finally {
			try {
				if( compressionOutputStream != null) {
					compressionOutputStream.close();
				}
				if( localFileInputStream != null) {
					localFileInputStream.close();
				}
			}catch(IOException e1){
				e1.printStackTrace();
			}
		}

		//System.out.println("Successfull Compression");
	}

}
