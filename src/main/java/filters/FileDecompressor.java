package filters;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

public class FileDecompressor {

	//Decompressing a local compressioned source file into a local destination file
	public static void decompressLocalFile(String localCompressionedSourceFile, String localDestinationFile, Configuration conf) {
		//String localSourceFile = "/home/ptcang/data/output/UserDetails.txt.bloom1";
		//String hdfsCompressionedFile = "/data/output/UserDetails.txt.bloom.gz";
		//String hdfsUri = "hdfs://localhost:9000/"; //String hdfsUri = "file:/";

		//FileSystem fileSystem = null;
		CompressionCodec compressionCodec = null;
		CompressionCodecFactory compressionCodecFactory = null;

		FileInputStream localFileInputStream = null;
		CompressionInputStream compressionInputStream = null;
		FileOutputStream localFileOutputStream = null;
		//FSDataInputStream fsDataInputStream = null; ///-----
		//InputStream inputStream = null;
		
		try {
			//fileSystem = FileSystem.get(new URI(hdfsUri), conf);
			//fileSystem = FileSystem.get(conf);
			compressionCodecFactory = new CompressionCodecFactory(conf);

			Path compressionedFilePath = new Path(localCompressionedSourceFile);
			//fsDataInputStream = fileSystem.open(compressionedFilePath); ////----------
			//inputStream =  new BufferedInputStream(new FileInputStream(new File(localCompressionedSourceFileName)));
			localFileInputStream =  new FileInputStream(localCompressionedSourceFile) ;
			
			/* get CompressionCodec instance associated with the file extension */
			compressionCodec = compressionCodecFactory.getCodec(compressionedFilePath);
			//compressionInputStream = compressionCodec.createInputStream(fsDataInputStream); ///-------
			compressionInputStream = compressionCodec.createInputStream(localFileInputStream );
			
			localFileOutputStream = new FileOutputStream(localDestinationFile);
			IOUtils.copyBytes(compressionInputStream, localFileOutputStream, conf, false);

			IOUtils.closeStream(compressionInputStream);
			IOUtils.closeStream(localFileOutputStream);
			IOUtils.closeStream(localFileInputStream);
		}catch(IOException e2) {
			e2.printStackTrace();
		}finally {
		}

		//System.out.println("Successfull Compression");

	}
	
	//Decompressing a hdfs compressioned source file into a local destination file
	public static void decompressHdfsFile(String hdfsCompressionedSourceFile, String localDestinationFile, Configuration conf) {
		//String localSourceFile = "/home/ptcang/data/output/UserDetails.txt.bloom1";
		//String hdfsCompressionedFile = "/data/output/UserDetails.txt.bloom.gz";
		//String hdfsUri = "hdfs://localhost:9000/"; //String hdfsUri = "file:/";

		FileSystem fileSystem = null;
		CompressionCodec compressionCodec = null;
		CompressionCodecFactory compressionCodecFactory = null;


		FileOutputStream localFileOutputStream = null;
		CompressionInputStream compressionInputStream = null;
		FSDataInputStream fsDataInputStream = null;
		try {
			//fileSystem = FileSystem.get(new URI(hdfsUri), conf);
			fileSystem = FileSystem.get(conf);
			compressionCodecFactory = new CompressionCodecFactory(conf);

			Path compressionedFilePath = new Path(hdfsCompressionedSourceFile);
			fsDataInputStream = fileSystem.open(compressionedFilePath);

			/* get CompressionCodec instance associated with the file extension */
			compressionCodec = compressionCodecFactory.getCodec(compressionedFilePath);
			compressionInputStream = compressionCodec.createInputStream(fsDataInputStream);

			localFileOutputStream = new FileOutputStream(localDestinationFile);
			IOUtils.copyBytes(compressionInputStream, localFileOutputStream, conf, false);

			IOUtils.closeStream(compressionInputStream);
			IOUtils.closeStream(localFileOutputStream);
			IOUtils.closeStream(fsDataInputStream);
		}catch(IOException e2) {
			e2.printStackTrace();
		}finally {
		}
		//System.out.println("Successfull Compression");
	}

}
