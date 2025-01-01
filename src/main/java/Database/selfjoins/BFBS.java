package Database.selfjoins;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import Database.pumakey.pumakey;
import filters.BallOfRadius;
import filters.CONST;
import filters.CONST_FILTERS;
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * Attention: only broadcast one variable possible
 * @author  tttquyen 
 *
 */

public class BFBS implements Serializable {
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;
	public static String hdfsPath=null;
	//public static String[] tmpsplits,tmpballsplits;
	//public static char[] tmpch;
	//public static String tmpstr,tmpline;

	public static BitSet set = new BitSet(CONST_FILTERS.SizeBF);
	public void closeSparkContext() {
		BFBS.javasparkcontext.close();
	}
	public BFBS() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("BFBS fuzzy self join");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
	}

	
	public static JavaPairRDD<String, String> createBallSplitsJavaPairRDD(JavaPairRDD<String, String> lines, int col,
			int eps, Broadcast<BitSet> bc) throws NullPointerException {
		return lines.flatMapToPair(new PairFlatMapFunction<Tuple2<String,String>, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String,String> t) throws Exception {
				String tmpstr = t._1;
				ArrayList<Tuple2<String, String>> tmpout = new ArrayList<>();
				//bf1.add(bc.value());
				set = bc.value();
				if (tmpstr != null && tmpstr.length() ==CONST.KEY_MINI_LENGTH) {
					String [] tmpsplits = pumakey.getIndex_Splits(tmpstr, eps);
					//if (eps==0) {
						//if(CONST_FILTERS.membershipTestBF(set, tmpstr))
							tmpout.add(new Tuple2<String, String>(tmpsplits[0], t._2));
					//} 
					//else {
					if (eps>0) {
						ArrayList<Integer> indexsplits = new ArrayList<>();
						for(int i=0; i<(eps);i++) indexsplits.add(i, i+1);
						char[] tmpch = tmpstr.toCharArray();
						
						//if(CONST_FILTERS.membershipTestBF(set, tmpstr)) {
							BallOfRadius.generateBallSplitBFtest(tmpch, tmpch, tmpch.length - 1, eps, tmpout, set,t._2,tmpsplits,indexsplits,eps);
						//}
					//}
					}
				}
				return tmpout.iterator();

			}
		});
	}
	
	
	public void fuzzyJoin(JavaPairRDD<String, String> pairRDD_02,
			String output, int eps) {
		
		JavaPairRDD<String, String> joinResult = pairRDD_02.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
				ArrayList<Tuple2<String, String>>  out = new ArrayList<Tuple2<String, String>> ();
//				if(!t._1.endsWith("R")) {
					ArrayList<String>  slist = new ArrayList<String> ();
					ArrayList<String>  tlist = new ArrayList<String> ();
					String skey, bkey;
					for(String str: t._2) {
						//skey=pumakey.getRKey(str, CONST.FIRST_COL);
						//if(skey.length()==CONST.LENGTH) {
							slist.add(str);
							tlist.add(str);
						//}
					}
					if(slist.size()>1) {
						slist.remove(slist.size()-1);
						tlist.remove(0);
	//					out.add(new Tuple2<String,String>(String.valueOf(slist.size()),String.valueOf(tlist.size())));
//						String skey,bkey;
						for(String s : slist) {
							skey = pumakey.getRecordKey(s, CONST.FIRST_COL);
							if(skey!=null && skey.length()==CONST.KEY_MINI_LENGTH) {
								for(String b : tlist) {
									bkey=pumakey.getRecordKey(b, CONST.FIRST_COL);
									if (bkey!=null && bkey.length()==CONST.KEY_MINI_LENGTH && pumakey.isSimilair(skey,bkey,eps))
										out.add(new Tuple2<String,String>(s,b));
								}
								tlist.remove(0);
							}
					}
				}
				return out.iterator();
			}
		});
			// Luu ra HDFS
			joinResult.saveAsTextFile(hdfsPath + output);
	}
	
	public static JavaPairRDD<String, String> createJavaPairRDD(JavaRDD<String> lines, int keyCol)
			throws NullPointerException {
		return lines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				ArrayList<Tuple2<String, String>> out = new ArrayList<>();
				String tmpstr = pumakey.getRecordKey(t, keyCol);
				if (tmpstr!=null && tmpstr.length() == CONST.KEY_MINI_LENGTH)
					out.add(new Tuple2<String, String>(tmpstr, t));
				return out.iterator();
			}
		});
	}
	
	public JavaRDD<Integer> keys(JavaPairRDD<String, String> lines) {
		return lines.keys().flatMap(new FlatMapFunction<String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Integer> call(String t) throws Exception {
				return CONST_FILTERS.hashkeyBF(t).iterator();
			}

		});//.distinct();
	}
	
	public void run(String input0, String output, int eps) {
		long ts = System.currentTimeMillis();
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0, CONST.NUM_PARTITION);
		JavaPairRDD<String, String> pairRDD = createJavaPairRDD(lines0, CONST.FIRST_COL);//.partitionBy(CONST.partition);
		pairRDD.persist(CONST.STORAGE_LEVEL);
		
		JavaRDD<Integer> keys = keys(pairRDD);
		BitSet set = new BitSet(CONST_FILTERS.SizeBF);

		List<Integer> list = keys.collect();
//		System.out.println("count list " + keys.count());
		for (int k : list) {
//			System.out.println("set " + k);
			set.set(k);
		}
		
		long te = System.currentTimeMillis();
		System.out.println("preproceessing running time " + (te - ts) / 1000 + " s");
		ts = System.currentTimeMillis();
		final Broadcast<BitSet> bc = javasparkcontext.broadcast(set);

		//JavaPairRDD<String, String> pairRDD_1 = createSplitsJavaPairRDD(lines0, CONST.SECOND_COL, eps, bc).partitionBy(CONST.partition);
		JavaPairRDD<String, String> pairRDD_0 = createBallSplitsJavaPairRDD(pairRDD, CONST.FIRST_COL, eps, bc).partitionBy(CONST.partition);
		
		fuzzyJoin(pairRDD_0, output, eps);
		//pairRDD_0.mapToPair(t->new Tuple2<String,String>(t._1, pumakey.getRKey(t._2, CONST.FIRST_COL))).groupByKey().saveAsTextFile(hdfsPath + output);
		te = System.currentTimeMillis();
		System.out.println("fuzzy join running time " + (te - ts) / 1000 + " s");
		
		javasparkcontext.close();
	}

	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 3) {
				System.err.println("Usage:BFBallSplit <path_to_dataset0>"
						+ "<path_to_output> <eps>");
				System.exit(2);
			}
			String inputPath0 = args[0];	//K: dataset0 Facebook
			String outputPath = args[1];

			int eps = Integer.parseInt(args[2]);
			
			BFBS sparkProgram = new BFBS();
			
			long start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, outputPath, eps);
			long end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
