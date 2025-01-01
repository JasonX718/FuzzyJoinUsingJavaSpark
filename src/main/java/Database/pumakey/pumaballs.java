package Database.pumakey;

//import java.io.DataOutputStream;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

//import filters.BitSetWritable;
import filters.CONST;
import filters.CONST_FILTERS;
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * 
 * @author  tttquyen 
 *
 */
public class pumaballs implements Serializable {
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;
	public static String hdfsPath=null;
	public static int index = 0;
	
	public void closeSparkContext() {
		pumaballs.javasparkcontext.close();
	}

	public pumaballs() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("Puma balls fuzzy join");//.setMaster(CONSTANTS.MASTER);
		//sparkconf.set("spark.executor.memory", "3g");
		//sparkconf.set("spark.executor.instances", "57");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
		
	}
	

	
	public static JavaRDD<String> createJavaRDD(JavaRDD<String> lines, int col) {
		return lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String t) throws Exception {
				ArrayList<String> out = new ArrayList<String>();
				String str = pumakey.getStrKey(t, col);
				if (str.length()==CONST.KEY_MINI_LENGTH && !str.endsWith("R") && !str.endsWith("S"))
					out.add(str);
				return out.iterator();
			}

		});//.repartition(CONSTANTS.NUM_PARTITION);//.distinct();
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
			    int ps = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(local).getBytes()) % CONST_FILTERS.vectorsize);
			    String tmpps = String.valueOf(ps);
			    ball = ball.concat(",").concat(tmpps);
			    //ball = ball.concat(",").concat(String.valueOf(local));
			    if(index > 0 && thres > 1) {
			    	ball = generateBall(ball,s, local, index - 1, thres - 1, ALPHABET);
			    }
			}
	        }//end for loop
		return ball;
    }//end generateBall

	public static void generateBall(BitSet ball,char[] s, char[] ms, int index, int thres,char[] ALPHABET) throws IOException, InterruptedException
    {
		/**
		 * index : length-1
		 * 
		 */
		char[] local = ms.clone();
		if(index > 0 && thres > 0) {
			generateBall(ball, s, ms, index - 1, thres,ALPHABET);
		}
		for(char value : ALPHABET)
	        {
	    	if(value != s[index])
			{
			    local[index] = value;
			    int ps = Math.abs(CONST_FILTERS.hashFunction.hash(String.valueOf(local).getBytes()) % CONST_FILTERS.vectorsize);
			    ball.set(ps);
			    if(index > 0 && thres > 1) {
			    	generateBall(ball,s, local, index - 1, thres - 1, ALPHABET);
			    }
			}
	        }//end for loop
    }//end generateBall

	public void run(String input0, String input1, String output, int eps) throws IOException {
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0, CONST.NUM_PARTITION);
		JavaRDD<String> lines1 = javasparkcontext.textFile(hdfsPath + input1, CONST.NUM_PARTITION);
		JavaRDD<String> pairRDD_0 = createJavaRDD(lines0, CONST.FIRST_COL);
		
		
		JavaRDD<String> pairRDD_1 = createJavaRDD(lines1, CONST.SECOND_COL);
		
		pairRDD_0 = pairRDD_0.union(pairRDD_1);//.distinct().repartition(CONSTANTS.NUM_PARTITION);
		
		
//		JavaPairRDD<String, String> balls = pairRDD_0.mapToPair(new PairFunction<String, String, String>() {
		JavaPairRDD<Integer, BitSet> balls = pairRDD_0.mapToPair(new PairFunction<String, Integer, BitSet>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, BitSet> call(String t) throws Exception {
				int ps = Math.abs(CONST_FILTERS.hashFunction.hash(t.getBytes()) % CONST_FILTERS.vectorsize);
			    //String ball = String.valueOf(ps);
				BitSet ball = new BitSet(CONST_FILTERS.vectorsize);
				ball.set(ps);
				generateBall(ball,t.toCharArray(), t.toCharArray(), t.length()-1, eps,CONST.ALPHABET);
			return new Tuple2<Integer, BitSet>(ps, ball);
			}
		});
		
		balls = balls.reduceByKey(new Function2<BitSet, BitSet, BitSet>() {
	
			private static final long serialVersionUID = 1L;

			@Override
			public BitSet call(BitSet hashballs, BitSet hashball) throws Exception {
				hashballs.or(hashball);
				return hashballs;
			}
		});
		
		Map<Integer, BitSet> map = balls.collectAsMap();
		HashMap<Integer, BitSet> hashmap = new HashMap<>();
		hashmap.putAll(map);
		//FileOutputStream fos = new FileOutputStream("/home/ubuntu/balllist.txt");
		FileSystem fs = FileSystem.get(javasparkcontext.hadoopConfiguration());	
		ObjectOutputStream oos = new ObjectOutputStream(fs.create(new Path(hdfsPath+output)));
		oos.writeObject(hashmap);
		oos.close();//fs.close();
		
		//balls.saveAsTextFile(hdfsPath+output+"/hashballs");
//		balls.saveAsHadoopFile(hdfsPath+output, IntWritable.class, BitSetWritable.class, TextOutputFormat.class);
		
//		balls.saveAsTextFile(hdfsPath+output+"/stringballs");
		
/*		pairRDD_1 = balls.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Tuple2<String, String> t) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<String> out = new ArrayList<>();
				String[] arr = t._2.split(",");
				for(String str : arr) {
					out.add(str);
				}
				return out.iterator();
			}
		}).distinct();
		pairRDD_1.persist(CONSTANTS.STORAGE_LEVEL);
		
		pairRDD_1.saveAsTextFile(hdfsPath+output+"/countstringballs");
		System.out.println("num count all string balls of dataset " + pairRDD_1.count());
*/
		javasparkcontext.close();
	}

	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 4) {
				System.err.println("Usage:pumaballs <path_to_dataset0> <path_to_dataset1>"
						+ "<path_to_output> <eps>");
				System.exit(2);
			}
			String inputPath0 = args[0];	
			String inputPath1 = args[1];	
			String outputPath = args[2];

			int eps = Integer.parseInt(args[3]);
			long start = 0;
			long end = 0;
			
			pumaballs sparkProgram = new pumaballs();
			
			start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, inputPath1, outputPath, eps);
			end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
