package Database.twoway;

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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import Database.pumakey.pumakey;
import filters.CONST;
import filters.CONST_FILTERS;
import filters.IntersectionBallListInt;
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * 
 * @author  tttquyen 
 *
 */
public class IFFNMPar implements Serializable {
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;
	public static String hdfsPath=null;
//	public static int index = 0;
	
	public void closeSparkContext() {
		IFFNMPar.javasparkcontext.close();
	}

	public IFFNMPar() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("IFF fuzzy 2way join");
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
				if (tmpstr!= null && tmpstr.length() == CONST.KEY_MINI_LENGTH)
					out.add(new Tuple2<String, String>(tmpstr, t));
					//out.add(new Tuple2<String, String>(tmpstr.substring(0, tmpstr.length()-1), t));
				return out.iterator();
			}
		});
	}
		
	public void run(String input1, String input2, String output, int eps) throws IOException, InterruptedException {
		long ts = System.currentTimeMillis();
		JavaRDD<String> lines1 = javasparkcontext.textFile(hdfsPath + input1);//, CONST.NUM_PARTITION);
		JavaRDD<String> lines2 = javasparkcontext.textFile(hdfsPath + input2);//, CONST.NUM_PARTITION);
		JavaPairRDD<String,String> pairRDD_1 = createJavaPairRDD(lines1, CONST.FIRST_COL);//.repartition(CONST.NUM_PARTITION);//.distinct();
		pairRDD_1.persist(CONST.STORAGE_LEVEL);
		
		JavaPairRDD<String,String> pairRDD_2 = createJavaPairRDD(lines2, CONST.SECOND_COL);//.repartition(CONST.NUM_PARTITION);//.distinct();
		pairRDD_2.persist(CONST.STORAGE_LEVEL);
		
		
		List<String> keys = pairRDD_1.keys().collect();
		BitSet set1 = new BitSet(CONST_FILTERS.vectorsize);
		for(String key: keys) {
			set1.set(Integer.valueOf(key));
		}
		
		keys = pairRDD_2.keys().collect();
		BitSet set2 = new BitSet(CONST_FILTERS.vectorsize);
		for(String key: keys) {
			set2.set(Integer.valueOf(key));
		}

		IntersectionBallListInt iff = new IntersectionBallListInt(set2, set1, eps);
		long te = System.currentTimeMillis(); 
		System.out.println("preproceessing running time " + (te - ts) / 1000 + " s");
		ts = System.currentTimeMillis();
		
		final Broadcast<IntersectionBallListInt> bcballs = javasparkcontext.broadcast(iff);

		JavaPairRDD<String, String> pair2 = pairRDD_2.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String,String>>, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Iterator<Tuple2<String,String>> t) throws Exception {
				ArrayList<Tuple2<String, String>> out = new ArrayList<>();
				IntersectionBallListInt iff = bcballs.value();
				while(t.hasNext()) {
					Tuple2<String,String> tuple = t.next();
					//int p = Math.abs(CONST_FILTERS.hashFunction.hash(tuple._1.getBytes())) % CONST_FILTERS.vectorsize;
					//if(tuple._1.length()==CONST.LENGTH) {
						int p=Integer.valueOf(tuple._1);
						ArrayList<Integer> ball = iff.getBall(p); 
							if(ball!=null && ball.size()>0) {
								for(int i:ball) {
									//if(i < p)
										out.add(new Tuple2<String, String>(pumakey.getKeyInt(i), tuple._2));
								}
							}
							//out.add(new Tuple2<String, Tuple2<String,String>>(String.valueOf(p), new Tuple2<String,String>("-1",tuple._2)));
					//}
				}
				return out.iterator();
			}
		});
		
		JavaPairRDD<String, String> pair1 = pairRDD_1.filter(new Function<Tuple2<String,String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> t) throws Exception {
				IntersectionBallListInt iff = bcballs.value();
				int p=Integer.valueOf(t._1);
				return iff.testBall2(p);
			}
		});
		
		JavaPairRDD<String, String> joinResult = pair1.join(pair2).flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<String,String>>, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> t) throws Exception {
				ArrayList<Tuple2<String, String>>  out = new ArrayList<Tuple2<String, String>> ();
				//String skey=pumakey.getRKey(t._2._1, CONST.FIRST_COL); 
				//String bkey=pumakey.getRKey(t._2._2, CONST.SECOND_COL); 
				//if (bkey.length()==CONST.LENGTH && skey.length()==CONST.LENGTH && pumakey.isSimilair(skey,bkey,eps)) {
					//out.add(new Tuple2<String,String>(skey,bkey));
					out.add(new Tuple2<String,String>(t._2._1,t._2._2));
				//}
				return out.iterator();
			}
		});
		
/*		JavaPairRDD<String, Tuple2<String,String>> joinResult = pair1.join(pair2).flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<String,String>>, String, Tuple2<String,String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Tuple2<String,String>>> call(Tuple2<String, Tuple2<String, String>> t) throws Exception {
				ArrayList<Tuple2<String, Tuple2<String,String>>>  out = new ArrayList<Tuple2<String, Tuple2<String,String>>> ();
				String skey=pumakey.getRKey(t._2._1, CONST.FIRST_COL); 
				String bkey=pumakey.getRKey(t._2._2, CONST.SECOND_COL); 
				//if (bkey.length()==CONST.LENGTH && skey.length()==CONST.LENGTH && pumakey.isSimilair(skey,bkey,eps)) {
					//out.add(new Tuple2<String,String>(skey,bkey));
					out.add(new Tuple2<String,Tuple2<String,String>>(t._1,new Tuple2<String,String>(skey,bkey)));
				//}
				return out.iterator();
			}
		});
*/			
			// Luu ra HDFS
			joinResult.saveAsTextFile(hdfsPath + output);
	
		te = System.currentTimeMillis();
		System.out.println("fuzzy join running time " + (te - ts) / 1000 + " s");
		javasparkcontext.close();
	}

	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 4) {
				System.err.println("Usage:FF <path_to_dataset0> <path_to_dataset1>"
						+ "<path_to_output> <eps>");
				System.exit(2);
			}
			String inputPath0 = args[0];
			String inputPath1 = args[1];
			String outputPath = args[2];

			int eps = Integer.parseInt(args[3]);
			long start = 0;
			long end = 0;
			
			IFFNMPar sparkProgram = new IFFNMPar();
			
			start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, inputPath1, outputPath, eps);
			end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
