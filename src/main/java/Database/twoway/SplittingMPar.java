package Database.twoway;

import java.io.Serializable;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
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
 * Attention: only broadcast one variable possible
 * @author  tttquyen 
 *
 */

public class SplittingMPar implements Serializable {
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;
	public static String hdfsPath=null;
//	public static String[] tmpsplits;
//	public static String tmpstr;

	public void closeSparkContext() {
		SplittingMPar.javasparkcontext.close();
	}
	public SplittingMPar() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("Splitting fuzzy 2way join");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
	}

	public static JavaPairRDD<String, String> createSplitsJavaPairRDD(JavaRDD<String> lines, int keyCol, int eps)
			throws NullPointerException {
		return lines.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Iterator<Tuple2<String, String>> call(Iterator<String> t) throws Exception {
				
				ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
					String tmpstr,tuple;//=pumakey.getRKey(t, keyCol);
						while(t.hasNext()) {
							tuple = t.next();
							tmpstr=pumakey.getRecordKey(tuple, keyCol);
						//if (tmpstr!=null && tmpstr.length()==CONST.LENGTH && !tmpstr.contains("R") && !tmpstr.contains("S")) {
						if (tmpstr!=null && tmpstr.length()==CONST.KEY_MINI_LENGTH) {
								try {
										String[] tmpsplits = pumakey.getIndex_Splits(tmpstr, eps);
										for(String tmp: tmpsplits) {
											//out.add(new Tuple2<String, String>(tmp, tmpstr));
											out.add(new Tuple2<String, String>(tmp, tuple));
									}
								}
								catch(Exception e) {
								}
								
							//} else out.add(new Tuple2<String, String>(tmpstr, tmpstr));
						}
					}
					return out.iterator();
				
			}
		});
	}
	
	public void fuzzyJoin(JavaPairRDD<String, String> pairRDD_02,JavaPairRDD<String, String> pairRDD_12,
			String output, int eps) {
		/*JavaPairRDD<String, String> joinResult = pairRDD_02.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
				ArrayList<Tuple2<String, String>>  out = new ArrayList<Tuple2<String, String>> ();
				//if(t._1.length()>0) {	
					ArrayList<String>  slist = new ArrayList<String> ();
					ArrayList<String>  tlist = new ArrayList<String> ();
					//String key;
					String skey, bkey;
					for(String str: t._2) {
						//key=pumakey.getRKey(str, CONST.FIRST_COL);
						//if(key.length()==CONST.LENGTH) {
							slist.add(str);
							tlist.add(str);
						//}
						}
					if(slist.size()>1) {
						slist.remove(slist.size()-1);
						tlist.remove(0);
						//String skey,bkey;
						for(String s : slist) {
							skey = pumakey.getRKey(s, CONST.FIRST_COL);
							if(skey!=null && skey.length()==CONST.LENGTH) {
								for(String b : tlist) {
									bkey=pumakey.getRKey(b, CONST.FIRST_COL);
									if (bkey!=null && bkey.length()==CONST.LENGTH && pumakey.isSimilair(skey,bkey,eps))
										out.add(new Tuple2<String,String>(s,b));
								}
								tlist.remove(0);
							}
						}
					}
				//}
				return out.iterator();
			}
		});*/
		
		JavaPairRDD<String, String> joinResult = pairRDD_02.join(pairRDD_12).flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<String,String>>, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> t) throws Exception {
				ArrayList<Tuple2<String, String>>  out = new ArrayList<Tuple2<String, String>> ();
				String skey=pumakey.getRecordKey(t._2._1, CONST.FIRST_COL); 
				String bkey=pumakey.getRecordKey(t._2._2, CONST.SECOND_COL); 
				if (bkey.length()==CONST.KEY_MINI_LENGTH && skey.length()==CONST.KEY_MINI_LENGTH && pumakey.isSimilair(skey,bkey,eps)) {
					//out.add(new Tuple2<String,String>(skey,bkey));
					out.add(new Tuple2<String,String>(t._2._1,t._2._2));
				}
				return out.iterator();
			}
		});
			// Luu ra HDFS
			joinResult.saveAsTextFile(hdfsPath + output);

	}
	

	public void run(String input0, String input1, String output, int eps) {
		
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0, CONST.NUM_PARTITION);
		
		JavaPairRDD<String, String> pairRDD_0 = createSplitsJavaPairRDD(lines0, CONST.FIRST_COL,eps);//.partitionBy(CONST.partition); // OK!
		
		JavaRDD<String> lines1 = javasparkcontext.textFile(hdfsPath + input1, CONST.NUM_PARTITION);
		
		JavaPairRDD<String, String> pairRDD_1 = createSplitsJavaPairRDD(lines1, CONST.SECOND_COL,eps);//.partitionBy(CONST.partition); // OK!
		
		
		//pairRDD_0.groupByKey().saveAsTextFile(hdfsPath + output);
		fuzzyJoin(pairRDD_0, pairRDD_1, output, eps);

		javasparkcontext.close();
	}

	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 4) {
				System.err.println("Usage:Splitting <path_to_dataset0>"
						+ "<path_to_output> <eps>");
				System.exit(2);
			}
			String inputPath0 = args[0];	//K: dataset0 Facebook
			String inputPath1 = args[1];
			String outputPath = args[2];

			int eps = Integer.parseInt(args[3]);
			
			SplittingMPar sparkProgram = new SplittingMPar();
			
			long start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, inputPath1, outputPath, eps);
			long end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}