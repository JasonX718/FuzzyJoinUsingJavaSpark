package Database.pumakey;

import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import filters.CONST;
import scala.Tuple2;

public class keycount implements Serializable{
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;
	public static String hdfsPath=null;
	public static int index = 0;
	public static JavaPairRDD<String,Integer> pairRDD_0,pairRDD_1;
	
	public void closeSparkContext() {
		pumaballs.javasparkcontext.close();
	}

	public keycount() throws ClassNotFoundException {
		//cleanFile();
		sparkconf = new SparkConf().setAppName("Key count");//.setMaster(CONSTANTS.MASTER);
		//sparkconf.set("spark.executor.memory", "1g");
		//sparkconf.set("spark.executor.instances", "30");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
		
	}
	
	public static JavaRDD<String> createJavaRDD(JavaRDD<String> lines, int col) {
		//return lines.map(t->pumakey.getStrKey(t, col)).filter(new Function<String, Boolean>() {
		return lines.map(t->pumakey.getRecordKey(t, col)).filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String t) throws Exception {
				// TODO Auto-generated method stub
				return t!=null && t.length()==CONST.KEY_MINI_LENGTH;
			}
		});//.distinct();
	}
	
	public static JavaPairRDD<String, Integer> createJavaPairRDD(JavaRDD<String> lines, int col) {
		return lines.mapToPair(t->new Tuple2<String,Integer>(pumakey.getCountKey(t, col),1));
	}
	
	public static JavaPairRDD<String, Integer> createpair(JavaRDD<String> lines) {
		return lines.mapToPair(t->new Tuple2<String,Integer>(t, 1));
	}
	/**
	 * count all key of dataset (not filter)
	 * @param input0
	 * @param input1
	 * @param output
	 */
	public void run(String input0, String input1, String output) {
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0);//, CONSTANTS.NUM_PARTITION).repartition(CONSTANTS.NUM_PARTITION); // OK!
		
		//JavaRDD<String> RDD_0 = createJavaRDD(lines0, CONSTANTS.FIRST_COL);//.repartition(1);//.partitionBy(new HashPartitioner(CONSTANTS.NUM_PARTITION)); // OK!
		//pairRDD_0 = createpair(RDD_0).reduceByKey((a,b)->a+b).repartition(1);
		
		//JavaRDD<String> RDD_1 = createJavaRDD(lines0, CONSTANTS.SECOND_COL);//.repartition(1);//.partitionBy(new HashPartitioner(CONSTANTS.NUM_PARTITION)); // OK!
		//pairRDD_1 = createpair(RDD_1).reduceByKey((a,b)->a+b).repartition(1);
		
		pairRDD_0 = createJavaPairRDD(lines0, CONST.FIRST_COL).reduceByKey((a,b)->a+b);
		pairRDD_1 = createJavaPairRDD(lines0, CONST.SECOND_COL).reduceByKey((a,b)->a+b);
		
		pairRDD_0.saveAsHadoopFile(hdfsPath+output+"/keycountcol_"+CONST.FIRST_COL, String.class, Integer.class, TextOutputFormat.class);
		pairRDD_1.saveAsHadoopFile(hdfsPath+output+"/keycountcol_"+CONST.SECOND_COL, String.class, Integer.class, TextOutputFormat.class);

		javasparkcontext.close();
	}
	
	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 3) {
				System.err.println("Usage:KeyCount <path_to_dataset0> <path_to_dataset1>"
						+ "<path_to_output>");
				System.exit(2);
			}
			String inputPath0 = args[0];	
			String inputPath1 = args[1];	
			String outputPath = args[2];

			long start = 0;
			long end = 0;
			
			keycount sparkProgram = new keycount();
			
			start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, inputPath1, outputPath);
			end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

}
