package Database.selfjoins;

import java.io.IOException;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import Database.pumakey.pumakey;
import filters.BallListInt;
import filters.CONST;
import filters.CONST_FILTERS;
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * 
 * @author  tttquyen 
 *
 */
public class FF_Built implements Serializable {
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;
	public static String hdfsPath=null;
//	public static int index = 0;
	
	public void closeSparkContext() {
		FF_Built.javasparkcontext.close();
	}

	public FF_Built() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("FF fuzzy self join built");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
		
	}
		
	public static JavaPairRDD<String, String> createJavaPairRDD(JavaRDD<String> lines, int keyCol)
			throws NullPointerException {
		return lines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				ArrayList<Tuple2<String, String>> out = new ArrayList<>();
				String tmpstr = pumakey.getRecordKey(t, keyCol);
				if (tmpstr.length() == CONST.KEY_MINI_LENGTH)
					out.add(new Tuple2<String, String>(tmpstr, t));
				return out.iterator();
			}
		});
	}
		
	public void run(String input0, String output, int eps) throws IOException, InterruptedException {
		long ts = System.currentTimeMillis();
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0, CONST.NUM_PARTITION);
		JavaPairRDD<String,String> pairRDD_0 = createJavaPairRDD(lines0, CONST.FIRST_COL);//.repartition(CONST.NUM_PARTITION);//.distinct();
		pairRDD_0.persist(CONST.STORAGE_LEVEL);
		
		List<String> keys = pairRDD_0.keys().collect();
		BitSet inputset = new BitSet(CONST_FILTERS.vectorsize);
		for(String key: keys) {
			inputset.set(Integer.valueOf(key));
		}

//		BallListInt ff = new BallListInt(input, eps);
		long te = System.currentTimeMillis(); 
		System.out.println("preproceessing running time " + (te - ts) / 1000 + " s");
		ts = System.currentTimeMillis();
		
		final Broadcast<BitSet> bcballs = javasparkcontext.broadcast(inputset);

		JavaPairRDD<String, Tuple2<String,String>> pair0 = pairRDD_0.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String,String>>, String, Tuple2<String,String>>() {
		
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Tuple2<String,String>>> call(Iterator<Tuple2<String,String>> t) throws Exception {
				ArrayList<Tuple2<String, Tuple2<String,String>>> out = new ArrayList<>();
				BitSet inputset = bcballs.value();
				BallListInt ff = new BallListInt(inputset, eps);
				while(t.hasNext()) {
					Tuple2<String,String> tuple = t.next();
					if(tuple._1.length()==CONST.KEY_MINI_LENGTH) {
						int p=Integer.valueOf(tuple._1);
						ArrayList<Integer> ball = ff.getBall(p); 
							if(ball!=null && ball.size()>0) {
								for(int i:ball) {
									if(i < p)
										out.add(new Tuple2<String, Tuple2<String,String>>(String.valueOf(i), new Tuple2<String,String>(String.valueOf(p),tuple._2)));
								}
						}
							out.add(new Tuple2<String, Tuple2<String,String>>(String.valueOf(p), new Tuple2<String,String>("-1",tuple._2)));
							
					}
				}
				return out.iterator();
			}
		});
		
		pair0.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Tuple2<String,String>>>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<Tuple2<String,String>>> t)
					throws Exception {
				ArrayList<Tuple2<String, String>>  out = new ArrayList<Tuple2<String, String>> ();
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
//				String skey,bkey;
				for (String s : s1list) {
//					skey=pumakey.getRKey(s, CONST.FIRST_COL);
//					if(skey.length()==CONST.LENGTH) {
						if(s2list.size()>0) {
							s2list.remove(0);
							for (String b : s2list) {
//								bkey=pumakey.getRKey(b, CONST.FIRST_COL);
//								if(bkey.length()==CONST.LENGTH)
									out.add(new Tuple2<String, String>(s, b));
								//out.add(new Tuple2<String, Tuple2<String,String>>(t._1, new Tuple2<String,String>(skey, bkey)));
							}
						}
						for (String b : blist) {
//							bkey=pumakey.getRKey(b, CONST.FIRST_COL);
//							if(bkey.length()==CONST.LENGTH)
								out.add(new Tuple2<String, String>(s, b));
							//out.add(new Tuple2<String, Tuple2<String,String>>(t._1, new Tuple2<String,String>(skey, bkey)));
						}
					}
//				} 
			//}
				return out.iterator();
			}
		}).saveAsTextFile(hdfsPath + output);
	
		te = System.currentTimeMillis();
		System.out.println("fuzzy join running time " + (te - ts) / 1000 + " s");
		javasparkcontext.close();
	}

	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 3) {
				System.err.println("Usage:pumaballs <path_to_dataset0> <path_to_dataset1>"
						+ "<path_to_output> <eps>");
				System.exit(2);
			}
			String inputPath0 = args[0];	
			String outputPath = args[1];

			int eps = Integer.parseInt(args[2]);
			long start = 0;
			long end = 0;
			
			FF_Built sparkProgram = new FF_Built();
			
			start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, outputPath, eps);
			end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
