package Database.twoway;

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

public class BFBH1 implements Serializable {
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
		BFBH1.javasparkcontext.close();
	}

	public BFBH1() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("BF BH1 fuzzy 2way join");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";

	}

	public static JavaPairRDD<String, String> createBallBHJavaPairRDD(JavaRDD<String> lines, int col,
			int eps, Broadcast<BitSet> bc) throws NullPointerException {
		return lines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				ArrayList<Tuple2<String, String>> tmpout = new ArrayList<>();
				String tmpstr = pumakey.getRecordKey(t, col);
				if (tmpstr!=null && tmpstr.length() == CONST.KEY_MINI_LENGTH)
				{
					set = bc.value();
					char[] tmpch = tmpstr.toCharArray();
					if(CONST_FILTERS.membershipTestBF(set, tmpstr)) 
						tmpout.add(new Tuple2<String, String>(tmpstr, t));
					BallOfRadius.generateBFBall2way(tmpch, tmpch, tmpch.length - 1, eps, tmpout, t, tmpstr,set);
				}
				return tmpout.iterator();

			}
		});
	}
	//Create a JavaPairRDD with (key,value) of the record
	public static JavaPairRDD<String, String> createJavaPairRDD(JavaRDD<String> lines, int keyCol)
			throws NullPointerException {
		// Create a JavaPairRDD with (key,value) of the record at first and sort it line by line
		return lines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override // (key,value) of the record
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				ArrayList<Tuple2<String, String>> out = new ArrayList<>();
				String tmpstr = pumakey.getRecordKey(t, keyCol); // t is a record, keyCol is the column of the key
				if (tmpstr!=null && tmpstr.length() == CONST.KEY_MINI_LENGTH) // if the key is not null and has the length of 10
					out.add(new Tuple2<String, String>(tmpstr, t)); // add the record to the output
				return out.iterator();
			}
		});
	}

	public void fuzzyJoin(JavaPairRDD<String, String> pairRDD_02, JavaPairRDD<String, String> pairRDD_12,
			String output, int eps) {
		//JavaPairRDD<String, Tuple2<String,String>> joinResult = pairRDD_02.groupByKey().flatMapToPair(
		//		new PairFlatMapFunction<Tuple2<String,Iterable<Tuple2<String,String>>>, String, Tuple2<String,String>>() {
		/*JavaPairRDD<String, String> joinResult = pairRDD_02.groupByKey().flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<Tuple2<String,String>>>, String, String>() {
				private static final long serialVersionUID = 1L;

			@Override
			//public Iterator<Tuple2<String, Tuple2<String,String>>> call(Tuple2<String,Iterable<Tuple2<String,String>>> t) throws Exception {
			public Iterator<Tuple2<String, String>> call(Tuple2<String,Iterable<Tuple2<String,String>>> t) throws Exception {
				//ArrayList<Tuple2<String, Tuple2<String,String>>>  out = new ArrayList<Tuple2<String, Tuple2<String,String>>> ();
				ArrayList<Tuple2<String, String>>  out = new ArrayList<Tuple2<String, String>> ();
				int count=0; //String tmp="";
				for(Tuple2<String,String> p:t._2) {
					count++;
					//tmp=p._1;
				}
				//if(count==1)
					//out.add(new Tuple2<String,String>(t._1,tmp));
				out.add(new Tuple2<String,String>(t._1,String.valueOf(count)));
//				if(t._1.length()==CONST.LENGTH) {
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
		});*/
		JavaPairRDD<String, String> joinResult = pairRDD_02.join(pairRDD_12).mapToPair(t->t._2);
			// Luu ra HDFS
			joinResult.saveAsTextFile(hdfsPath + output);
//		}
	}

	public JavaRDD<Integer> keys(JavaPairRDD<String, String> lines) {
		//JavaRDD<String> tmp = tmppair.map(t -> t._1);//.distinct();
		return lines.keys().flatMap(new FlatMapFunction<String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Integer> call(String t) throws Exception {
				return CONST_FILTERS.hashkeyBF(t).iterator();
			}

		});//.distinct();
	}
	public void run(String input0, String input1, String output, int eps) {
		long ts = System.currentTimeMillis();

		// Read datasets and defined the number of partitions
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0, CONST.NUM_PARTITION);
		JavaRDD<String> lines1 = javasparkcontext.textFile(hdfsPath + input1, CONST.NUM_PARTITION);

		//JavaPairRDD<String, String> pairRDD = createJavaPairRDD(lines0, CONST.FIRST_COL);//.partitionBy(CONST.partition);

		// Create (key, value) from each lines
		JavaPairRDD<String, String> pairRDD1 = createJavaPairRDD(lines0, CONST.FIRST_COL);//.partitionBy(CONST.partition);
		pairRDD1.persist(CONST.STORAGE_LEVEL);
		
		JavaRDD<Integer> keys = keys(pairRDD1);
		BitSet set = new BitSet(CONST_FILTERS.SizeBF);

		// Store all key in a bitset
		List<Integer> list = keys.collect(); // collect all keys
//		System.out.println("count list " + keys.count());
		for (int k : list) { //add all keys to bitset
// 			System.out.println("set " + k);
			set.set(k);
		}

		long te = System.currentTimeMillis();
		System.out.println("preproceessing running time " + (te - ts) / 1000 + " s");
		ts = System.currentTimeMillis();

//		System.out.println("set value " + set.toString());
		final Broadcast<BitSet> bc = javasparkcontext.broadcast(set); // broadcast bitset to all nodes
//		System.out.println("bc value  " + bc.value().toString());

		JavaPairRDD<String, String> pairRDD_0 = createBallBHJavaPairRDD(lines1,CONST.SECOND_COL, eps, bc).partitionBy(CONST.partition);
		
		fuzzyJoin(pairRDD_0, pairRDD1, output, eps);
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
			if (args.length < 4) {
				System.err.println("Usage:BFBH1 <path_to_dataset0> <path_to_output> <eps>");
				System.exit(2);
			}
			String inputPath0 = args[0]; // K: dataset0 Facebook
			String inputPath1 = args[1];
			String outputPath = args[2]; //

			int eps = Integer.parseInt(args[3]);

			BFBH1 sparkProgram = new BFBH1();

			long start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, inputPath1, outputPath, eps);
			long end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}