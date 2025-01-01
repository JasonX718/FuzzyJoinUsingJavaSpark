package Database.selfjoins;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

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
 * Similarity join in Spark. Attention: only broadcast one variable possible
 * 
 * @author tttquyen
 *
 */

public class BFBH1NPar implements Serializable {
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;
	public static String hdfsPath = null;
	//public static char[] tmpch;
	//public static String tmpstr;
	/*public static BloomFilter bf1 = new BloomFilter(CONSTANTS_FILTERS.SizeBF, CONSTANTS_FILTERS.nbHashFuncBF,
			CONSTANTS_FILTERS.hashType);
*/
	public static BitSet set = new BitSet(CONST_FILTERS.SizeBF);
	public void closeSparkContext() {
		BFBH1NPar.javasparkcontext.close();
	}

	public BFBH1NPar() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("BF BH1 fuzzy self join Mappartition");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";

	}

	public static JavaPairRDD<String, Tuple2<String,String>> createBallBHJavaPairRDD(JavaPairRDD<String, String> lines, int col,
			int eps, Broadcast<BitSet> bc) throws NullPointerException {
		return lines.flatMapToPair(new PairFlatMapFunction<Tuple2<String,String>, String, Tuple2<String,String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Tuple2<String,String>>> call(Tuple2<String,String> t) throws Exception {
				String tmpstr = t._1;
				ArrayList<Tuple2<String, Tuple2<String,String>>> tmpout = new ArrayList<>();
				set = bc.value();
				if (tmpstr != null && tmpstr.length() == CONST.KEY_MINI_LENGTH) {
					char[] tmpch = tmpstr.toCharArray();
					//if(CONST_FILTERS.membershipTestBF(set, tmpstr)) {
						boolean b=false;
						b = BallOfRadius.generateBFBallTuple(tmpch, tmpch, tmpch.length - 1, eps, tmpout, t._2, b, t._1,set);
						//if(b) 
							tmpout.add(new Tuple2<String, Tuple2<String,String>>(tmpstr, new Tuple2<String,String>("-1",t._2)));
					//BallOfRadius.generateBHBallBFtest(tmpch, tmpch, tmpch.length - 1, eps, tmpout, set,t);
					//tmpout.add(new Tuple2<String, Tuple2<String,String>>(tmpstr, new Tuple2<String,String>("-1",t._2)));
					//}
				}
				return tmpout.iterator();

			}
		});
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

	public void fuzzyJoin(JavaPairRDD<String, Tuple2<String,String>> pairRDD_02,
			String output, int eps) {
		//JavaPairRDD<String, Tuple2<String,String>> joinResult = pairRDD_02.groupByKey().flatMapToPair(
		//		new PairFlatMapFunction<Tuple2<String,Iterable<Tuple2<String,String>>>, String, Tuple2<String,String>>() {
		JavaPairRDD<String, String> joinResult = pairRDD_02.groupByKey().flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<Tuple2<String,String>>>, String, String>() {
				private static final long serialVersionUID = 1L;

			@Override
			//public Iterator<Tuple2<String, Tuple2<String,String>>> call(Tuple2<String,Iterable<Tuple2<String,String>>> t) throws Exception {
			public Iterator<Tuple2<String, String>> call(Tuple2<String,Iterable<Tuple2<String,String>>> t) throws Exception {
				//ArrayList<Tuple2<String, Tuple2<String,String>>>  out = new ArrayList<Tuple2<String, Tuple2<String,String>>> ();
				ArrayList<Tuple2<String, String>>  out = new ArrayList<Tuple2<String, String>> ();
/*				int count=0; //String tmp="";
				for(Tuple2<String,String> p:t._2) {
					count++;
					//tmp=p._1;
				}
				//if(count==1)
					//out.add(new Tuple2<String,String>(t._1,tmp));
				out.add(new Tuple2<String,String>(t._1,String.valueOf(count)));
*///				if(t._1.length()==CONST.LENGTH) {
					ArrayList<String> s1list = new ArrayList<String>();
					ArrayList<String> s2list = new ArrayList<String>();
					ArrayList<String> blist = new ArrayList<String>();
					for(Tuple2<String,String> p : t._2) {
						//tmpstr = pumakey.getRKey(p._2, CONST.FIRST_COL);
						//if(tmpstr!=null && tmpstr.length()==CONST.LENGTH) {
							if(p._1.compareTo("-1")==0) {
								s1list.add(p._2);
							}
							else blist.add(p._2);
						//}
					}
					s2list.addAll(s1list);
//					String skey,bkey;
					for (String s : s1list) {
//						skey=pumakey.getRKey(s, CONST.FIRST_COL);
//						if(skey.length()==CONST.LENGTH) {
							if(s2list.size()>0) {
								s2list.remove(0);
								for (String b : s2list) {
//									bkey=pumakey.getRKey(b, CONST.FIRST_COL);
//									if(bkey.length()==CONST.LENGTH)
										out.add(new Tuple2<String, String>(s, b));
									//out.add(new Tuple2<String, Tuple2<String,String>>(t._1, new Tuple2<String,String>(skey, bkey)));
								}
							}
							for (String b : blist) {
//								bkey=pumakey.getRKey(b, CONST.FIRST_COL);
//								if(bkey.length()==CONST.LENGTH)
									out.add(new Tuple2<String, String>(s, b));
								//out.add(new Tuple2<String, Tuple2<String,String>>(t._1, new Tuple2<String,String>(skey, bkey)));
							}
						}
//					} 
				//}
				return out.iterator();
			}
		});
			// Luu ra HDFS
			joinResult.saveAsTextFile(hdfsPath + output);
//		}
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
					if(tmpstr!=null && tmpstr.length()==CONST.KEY_MINI_LENGTH) {
						for (int i = 0, initval = 0; i < CONST_FILTERS.nbHashFuncBF; i++) {
							initval = CONST_FILTERS.hashFunction.hash(tmpstr.getBytes(), initval);
							tmpset.set(Math.abs(initval) % CONST_FILTERS.SizeBF);
						}
						
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
	public void run(String input0, String output, int eps) {
		long ts = System.currentTimeMillis();
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0);//, CONST.NUM_PARTITION);

		JavaPairRDD<String, String> pairRDD = createJavaPairRDD(lines0, CONST.FIRST_COL);//.partitionBy(CONST.partition);
		pairRDD.persist(CONST.STORAGE_LEVEL);
		
		JavaRDD<String> keys = keys(pairRDD);
		BitSet set = new BitSet(CONST_FILTERS.SizeBF);

		List<String> list = keys.collect();
//		System.out.println("count list " + keys.count());
		for(String keyarr: list) {
			if(keyarr.length()>0) {
				String[] arr = keyarr.split(", ");
				for(String k:arr)
					set.set(Integer.valueOf(k));
			}
		}

		long te = System.currentTimeMillis();
		System.out.println("preproceessing running time " + (te - ts) / 1000 + " s");
		ts = System.currentTimeMillis();
//		System.out.println("set value " + set.toString());
		final Broadcast<BitSet> bc = javasparkcontext.broadcast(set);
//		System.out.println("bc value  " + bc.value().toString());

		JavaPairRDD<String, Tuple2<String,String>> pairRDD_0 = createBallBHJavaPairRDD(pairRDD,CONST.FIRST_COL, eps, bc).partitionBy(CONST.partition);
		
		fuzzyJoin(pairRDD_0, output, eps);
//		pairRDD_0.mapToPair(t->new Tuple2<String,String>(t._1, t._2._1)).groupByKey().saveAsTextFile(hdfsPath + output);
//		pairRDD_0.mapToPair(t->new Tuple2<String,Tuple2<String,String>>(t._1, new Tuple2<String,String>(t._2._1,pumakey.getRKey(t._2._2, CONST.FIRST_COL)))).groupByKey().saveAsTextFile(hdfsPath + output);
		//pairRDD_0.groupByKey().filter(t->t._1.length()==CONST.LENGTH).repartition(1).saveAsTextFile(hdfsPath + output);
//		System.out.println(pairRDD_0.groupByKey().count());
//		System.out.println(pairRDD_0.groupByKey().filter(t->t._1.length()==CONST.LENGTH).count());
		te = System.currentTimeMillis();
		System.out.println("fuzzy join running time " + (te - ts) / 1000 + " s");
		javasparkcontext.close();
	}

	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 3) {
				System.err.println("Usage:BFBH1 <path_to_dataset0> <path_to_output> <eps>");
				System.exit(2);
			}
			String inputPath0 = args[0]; // K: dataset0 Facebook
			String outputPath = args[1];

			int eps = Integer.parseInt(args[2]);

			BFBH1NPar sparkProgram = new BFBH1NPar();

			long start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, outputPath, eps);
			long end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
