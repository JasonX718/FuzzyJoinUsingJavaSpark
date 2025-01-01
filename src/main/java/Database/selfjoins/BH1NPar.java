package Database.selfjoins;

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
import filters.BallOfRadius;
import filters.CONST;
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * Attention: only broadcast one variable possible
 * @author  tttquyen 
 *
 */

public class BH1NPar implements Serializable {
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;
	public static String hdfsPath=null;
	//public static char[] tmpch;
	//public static String tmpstr,tmp;

	public void closeSparkContext() {
		BH1NPar.javasparkcontext.close();
	}
	//Accumulator
	//persist
	//sequenceFile[K, V]
	//rdd.collect().foreach(println)-> rdd.take(100).foreach(println)
	public BH1NPar() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("BH1 fuzzy self join");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
	}


	public static JavaPairRDD<String, Tuple2<String,String>> createBHJavaPairRDD(JavaRDD<String> lines, int keyCol, int eps)
			throws NullPointerException {
		return lines.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String,String>>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Iterator<Tuple2<String, Tuple2<String,String>>> call(String t) throws Exception {
				
				ArrayList<Tuple2<String, Tuple2<String,String>>> out = new ArrayList<Tuple2<String, Tuple2<String,String>>> ();
				String tmpstr=pumakey.getRecordKey(t, keyCol);
				if (tmpstr!=null && tmpstr.length()==(CONST.KEY_MINI_LENGTH)) {
//						if(!tmpstr.endsWith("R")) {
							try {
								char[] tmpch = tmpstr.toCharArray();
								boolean b=false;
								b=BallOfRadius.generateBHBallTuple(tmpch, tmpch, tmpch.length-1, eps, out,t,b,tmpstr); //Corriger 0
								//if (b) 
									out.add(new Tuple2<String, Tuple2<String,String>>(tmpstr, new Tuple2<String,String>("-1",t)));
								//if (b) out.add(new Tuple2<String, Tuple2<String,String>>(tmpstr, new Tuple2<String,String>("-1",tmpstr)));
							} catch(Exception e) {
								
							}
//						}
						//else out.add(new Tuple2<String, Tuple2<String,String>>(tmpstr, new Tuple2<String,String>("R",tmpstr)));
//						else out.add(new Tuple2<String, Tuple2<String,String>>(tmpstr, new Tuple2<String,String>("R",t)));
					}
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
	

	public void run(String input0, String output, int eps) {
		
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0);//, CONST.NUM_PARTITION); // OK!
		
		JavaPairRDD<String, Tuple2<String,String>> pairRDD_0 = createBHJavaPairRDD(lines0, CONST.FIRST_COL,eps);//.partitionBy(CONST.partition); // OK!
				
		fuzzyJoin(pairRDD_0, output, eps);
//		pairRDD_0.mapToPair(t->new Tuple2<String,String>(t._1, t._2._1)).groupByKey().saveAsTextFile(hdfsPath + output);
//		pairRDD_0.mapToPair(t->new Tuple2<String,Tuple2<String,String>>(t._1, new Tuple2<String,String>(t._2._1,pumakey.getRKey(t._2._2, CONST.FIRST_COL)))).groupByKey().saveAsTextFile(hdfsPath + output);
	//	pairRDD_0.groupByKey().filter(t->t._1.length()==CONST.LENGTH).repartition(1).saveAsTextFile(hdfsPath + output);
		
//		System.out.println(pairRDD_0.groupByKey().count());
//		System.out.println(pairRDD_0.groupByKey().filter(t->t._1.length()==CONST.LENGTH).count());
		javasparkcontext.close();
	}

	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 3) {
				System.err.println("Usage:BH1 <path_to_dataset0>"
						+ "<path_to_output> <eps>");
				System.exit(2);
			}
			String inputPath0 = args[0];	//K: dataset0 Facebook
			String outputPath = args[1];

			int eps = Integer.parseInt(args[2]);
			
			BH1NPar sparkProgram = new BH1NPar();
			
			long start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, outputPath, eps);
			long end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
