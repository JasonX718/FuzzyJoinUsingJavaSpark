package Database.twoway;

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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import Database.pumakey.pumakey;
import filters.CONST;
import filters.CONST_FILTERS;
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * Attention: only broadcast one variable possible
 * @author  tttquyen 
 *
 */

public class IBFSMPar implements Serializable {
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
		IBFSMPar.javasparkcontext.close();
	}
	public IBFSMPar() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("IBFS fuzzy 2way join");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
	}
	
	public void fuzzyJoin(JavaPairRDD<String, String> pairRDD_02,JavaPairRDD<String, String> pairRDD_12,
			String output, int eps) {
		
		JavaPairRDD<String, String> joinResult = pairRDD_02.join(pairRDD_12).flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<String,String>>, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> t) throws Exception {
				ArrayList<Tuple2<String, String>>  out = new ArrayList<Tuple2<String, String>> ();
				String skey=pumakey.getRecordKey(t._2._1, CONST.SECOND_COL); 
				String bkey=pumakey.getRecordKey(t._2._2, CONST.FIRST_COL); 
				if (bkey.length()==CONST.KEY_MINI_LENGTH && skey.length()==CONST.KEY_MINI_LENGTH && pumakey.isSimilair(skey,bkey,eps)) {
					//out.add(new Tuple2<String,String>(skey,bkey));
					out.add(new Tuple2<String,String>(t._2._1,t._2._2));
				}
				return out.iterator();
			}
		});			// Luu ra HDFS
			joinResult.saveAsTextFile(hdfsPath + output);
	}
	
	public static JavaPairRDD<String, String> createJavaPairRDD(JavaRDD<String> lines, int keyCol)
			throws NullPointerException {
		return lines.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Iterator<String> t) throws Exception {
				ArrayList<Tuple2<String, String>> out = new ArrayList<>();
				String tmpstr,tuple;
				while(t.hasNext()) {
					tuple=t.next();
					tmpstr = pumakey.getRecordKey(tuple, keyCol);
					if (tmpstr!=null && tmpstr.length() == CONST.KEY_MINI_LENGTH)
						out.add(new Tuple2<String, String>(tmpstr, tuple));
				}
				return out.iterator();
			}
		});
	}
	
	public static JavaPairRDD<String, String> createSplitsJavaPairRDD(JavaRDD<String> lines, int eps,int keyCol)
			throws NullPointerException {
		return lines.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Iterator<Tuple2<String, String>> call(Iterator<String> t) throws Exception {
				
				ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
				String tmpstr,tuple;
				while(t.hasNext()) {
					tuple=t.next();
					tmpstr=pumakey.getRecordKey(tuple, keyCol);
					if(tmpstr!=null && tmpstr.length() == CONST.KEY_MINI_LENGTH) {
							try {
									String[] tmpsplits = pumakey.getIndex_Splits(tmpstr, eps);
									for(String tmp: tmpsplits) {
										//out.add(new Tuple2<String, String>(tmp, tmpstr));
										out.add(new Tuple2<String, String>(tmp, tuple));
								}
							}
							catch(Exception e) {
							}
				}
							
						//} else out.add(new Tuple2<String, String>(tmpstr, tmpstr));
				}
					return out.iterator();
				
			}
		});
	}
	
	public JavaRDD<String> keys(JavaPairRDD<String, String> lines) {
		//JavaRDD<String> tmp = tmppair.map(t -> t._1);//.distinct();
		return lines.keys().mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Iterator<String> t) throws Exception {
				ArrayList<String> out = new ArrayList<>();
				BitSet tmpset = new BitSet(CONST_FILTERS.SizeBF);
				String tmpstr;
				while(t.hasNext()) {
					tmpstr = t.next();
					//if(tmpstr!=null && tmpstr.length()==CONST.LENGTH) {
						for (int i = 0, initval = 0; i < CONST_FILTERS.nbHashFuncBF; i++) {
							initval = CONST_FILTERS.hashFunction.hash(tmpstr.getBytes(), initval);
							tmpset.set(Math.abs(initval) % CONST_FILTERS.SizeBF);
					//	}
						
					}
				}
				String str = tmpset.toString();
				str=str.substring(1,str.length()-1);
				if(str.length()>0) out.add(str);
				return out.iterator();
				//return CONST_FILTERS.hashkeyBF(t).iterator();
			}

		});//.distinct();
	}
	
	public void run(String input0, String input1, String output, int eps) {
		long ts = System.currentTimeMillis();
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0, CONST.NUM_PARTITION);
		JavaRDD<String> lines1 = javasparkcontext.textFile(hdfsPath + input1, CONST.NUM_PARTITION);

		//JavaPairRDD<String, String> pairRDD = createJavaPairRDD(lines0, CONST.FIRST_COL);//.partitionBy(CONST.partition);
		
		JavaPairRDD<String, String> pairRDD1 = createSplitsJavaPairRDD(lines0, eps, CONST.FIRST_COL);//.partitionBy(CONST.partition);
		pairRDD1.persist(CONST.STORAGE_LEVEL);
		
		JavaRDD<String> keys = keys(pairRDD1);
		BitSet set1 = new BitSet(CONST_FILTERS.SizeBF);

		List<String> list = keys.collect();
//		System.out.println("count list " + keys.count());
		for(String keyarr: list) {
			if(keyarr.length()>0) {
				String[] arr = keyarr.split(", ");
				for(String k:arr)
					set1.set(Integer.valueOf(k));
			}
		}

		JavaPairRDD<String, String> pairRDD0 = createSplitsJavaPairRDD(lines1, eps, CONST.SECOND_COL);//.partitionBy(CONST.partition);
		pairRDD0.persist(CONST.STORAGE_LEVEL);
		
		keys = keys(pairRDD0);
		BitSet set0 = new BitSet(CONST_FILTERS.SizeBF);

		list = keys.collect();
//		System.out.println("count list " + keys.count());
		for(String keyarr: list) {
			if(keyarr.length()>0) {
				String[] arr = keyarr.split(", ");
				for(String k:arr)
					set0.set(Integer.valueOf(k));
			}
		}
		
		set = new BitSet(CONST_FILTERS.SizeBF);
		set.or(set0);
		set.and(set1);
		
		long te = System.currentTimeMillis();
		System.out.println("preproceessing running time " + (te - ts) / 1000 + " s");
		ts = System.currentTimeMillis();
//		System.out.println("set value " + set.toString());
		final Broadcast<BitSet> bc = javasparkcontext.broadcast(set);
//		System.out.println("bc value  " + bc.value().toString());
//		JavaPairRDD<String, String> pairRDD_1 = createSplitsJavaPairRDD(pairRDD1,eps).partitionBy(CONST.partition);
//		JavaPairRDD<String, String> pairRDD_0 = createBallSplitsJavaPairRDD(lines1, CONST.SECOND_COL, eps, bc).partitionBy(CONST.partition);
		
		pairRDD0=pairRDD0.filter(new Function<Tuple2<String,String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> t) throws Exception {
				set = bc.value();
				return CONST_FILTERS.membershipTestBF(set,t._1);
			}
		});
		
		pairRDD1=pairRDD1.filter(new Function<Tuple2<String,String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> t) throws Exception {
				set = bc.value();
				return CONST_FILTERS.membershipTestBF(set,t._1);
			}
		});
		
		fuzzyJoin(pairRDD0, pairRDD1, output, eps);
		//pairRDD_0.mapToPair(t->new Tuple2<String,String>(t._1, pumakey.getRKey(t._2, CONST.FIRST_COL))).groupByKey().saveAsTextFile(hdfsPath + output);
		te = System.currentTimeMillis();
		System.out.println("fuzzy join running time " + (te - ts) / 1000 + " s");
		
		javasparkcontext.close();
	}

	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 4) {
				System.err.println("Usage:BFBallSplit <path_to_dataset0>"
						+ "<path_to_output> <eps>");
				System.exit(2);
			}
			String inputPath0 = args[0];	//K: dataset0 Facebook
			String inputPath1 = args[1];
			String outputPath = args[2];

			int eps = Integer.parseInt(args[3]);
			
			IBFSMPar sparkProgram = new IBFSMPar();
			
			long start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, inputPath1, outputPath, eps);
			long end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
