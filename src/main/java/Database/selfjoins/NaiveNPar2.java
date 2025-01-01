package Database.selfjoins;

import java.io.Serializable;
//import java.util.ArrayList;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import Database.pumakey.pumakey;
import filters.CONST;
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * Naive algorithm
 * 
 * @author  tttquyen 
 *
 */
public class NaiveNPar2 implements Serializable {
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;
	//public static int J = (int)Math.sqrt((double)CONSTANTS.NUM_EXECUTORS);
	//public static int J = 10;
	public static int J = 6;
	public static String hdfsPath=null;
	
	public void closeSparkContext() {
		NaiveNPar2.javasparkcontext.close();
	}

	public NaiveNPar2() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("Naive fuzzy self join");//.setMaster(CONSTANTS.MASTER);
		//sparkconf.set("fs.defaultFS", "hdfs://quyen-master:9000");
		//sparkconf.set("spark.executor.memory", "1g");
		//sparkconf.set("spark.executor.instances", "54");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
		
	}


	public static JavaPairRDD<String, String> createJavaPairRDD(JavaRDD<String> lines, int keyCol)
			throws NullPointerException {
		return lines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
				String str = pumakey.getRecordKey(t, keyCol);
				
				if (str!=null && str.length()==CONST.KEY_MINI_LENGTH) {
//					if(!str.endsWith("R")) {
						//System.out.println("flatmap  " + str);
						//int i = pumakey.getHashCode(str.substring(0, str.length()-1).toCharArray(),J);
					int i = pumakey.getHashCode(str.toCharArray(),J);
					//int i = pumakey.getHashCode(t.toCharArray(),J);
					
		 	 			for(int j = i; j < J; j++) {
		 	 				//out.add(new Tuple2<String, String>(i+"_"+j, str));
		 	 				out.add(new Tuple2<String, String>(i+"_"+j, t));
		 	 				//System.out.println("key (" + i + ", " + j + ") with value (" + str + ")");
		 	 			}
		 	 			for(int j = 0; j < i; j++) {
		 	 				//out.add(new Tuple2<String, String> (j+"_"+i, str));
		 	 				out.add(new Tuple2<String, String> (j+"_"+i, t));
		 	 				//System.out.println("key (" + j + ", " + i + ") with value (" + str + ")");
		 	 			}
//					} else out.add(new Tuple2<String, String>(str, t));
				}
				return out.iterator();
			}
		});
	}
	

	public void fuzzyJoin(JavaPairRDD<String, String> pairRDD_02,
			String output, int eps) {
		JavaPairRDD<String, String> joinResult = pairRDD_02.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t)
					throws Exception {
				ArrayList<Tuple2<String, String>>  out = new ArrayList<Tuple2<String, String>> ();
//				if(!t._1.endsWith("R")) {
					ArrayList<String>  slist = new ArrayList<String> ();
					
					slist.addAll((Collection<? extends String>) t._2());
					if(slist.size()>1) {
						slist.remove(slist.size()-1);
						ArrayList<String>  tlist = new ArrayList<String> ();
						tlist.addAll((Collection<? extends String>) t._2());
						tlist.remove(0);
	//					out.add(new Tuple2<String,String>(String.valueOf(slist.size()),String.valueOf(tlist.size())));
						String skey,bkey;
						for(String s : slist) {
							skey=pumakey.getRecordKey(s, CONST.FIRST_COL);
							if(skey.length()==CONST.KEY_MINI_LENGTH) {
								for(String b : tlist) {
									bkey=pumakey.getRecordKey(b, CONST.FIRST_COL);
									if (bkey.length()==CONST.KEY_MINI_LENGTH && pumakey.isSimilair(skey,bkey,eps)) {
										//out.add(new Tuple2<String,String>(skey,bkey));
										out.add(new Tuple2<String,String>(s,b));
									}
								}
								tlist.remove(0);
							}
						}
//					}
				}
				return out.iterator();
			}
		});
			
			// Luu ra HDFS
			joinResult.saveAsTextFile(hdfsPath + output);
//		pairRDD_02.saveAsTextFile(hdfsPath + output);
//		pairRDD_02.groupByKey().saveAsTextFile(hdfsPath + output);
//			JavaRDD<Tuple2<String, String>> outputhdfs = joinResult.map(t->t._2).distinct();
//			outputhdfs.saveAsTextFile(hdfsPath + output);

//		}
	}

	public void run(String input0, String output, int eps) {
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0);//, CONST.NUM_PARTITION);
		
		JavaPairRDD<String, String> pairRDD_0 = createJavaPairRDD(lines0, CONST.FIRST_COL);//.partitionBy(new HashPartitioner(15)); // OK!
		
		fuzzyJoin(pairRDD_0, output, eps);

		javasparkcontext.close();
	}

	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 3) {
				System.err.println("Usage:SparkFuzzyJoinNaive <path_to_dataset>"
						+ "<path_to_output> <eps>");
				System.exit(2);
			}
			String inputPath0 = args[0];	//K: dataset0 Facebook
			String outputPath = args[1];

			int eps = Integer.parseInt(args[2]);
			//nameNode = args[4];
			long start = 0;
			long end = 0;
			
			NaiveNPar2 sparkProgram = new NaiveNPar2();
			
			start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, outputPath, eps);
			end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
