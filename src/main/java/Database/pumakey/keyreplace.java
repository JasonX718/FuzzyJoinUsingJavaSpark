package Database.pumakey;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import filters.CONST;
import scala.Tuple2;

public class keyreplace implements Serializable{
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;
	public static String hdfsPath=null;
	public static int index = 0;
	public static JavaPairRDD<String,String> pairRDD_0,pairRDD_1;
	
	public void closeSparkContext() {
		pumaballs.javasparkcontext.close();
	}

	public keyreplace() throws ClassNotFoundException {
		//cleanFile();
		sparkconf = new SparkConf().setAppName("Key replace");//.setMaster(CONSTANTS.MASTER);
		//sparkconf.set("spark.executor.memory", "1g");
		//sparkconf.set("spark.executor.instances", "30");
		javasparkcontext = new JavaSparkContext(sparkconf);
		hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
		
	}
	
	public static JavaPairRDD<String,String> createJavaRDD(JavaRDD<String> lines, int col) {
		return lines.mapToPair(t->new Tuple2<String,String>(pumakey.getCountKey(t, col),t));
	}

	
	public void run(String input0, String replace0, int col, String output) {
		JavaRDD<String> lines0 = javasparkcontext.textFile(hdfsPath + input0);//, CONSTANTS.NUM_PARTITION);//.repartition(CONSTANTS.NUM_PARTITION); // OK!
		pairRDD_0 = createJavaRDD(lines0, col );//.repartition(1);//.partitionBy(new HashPartitioner(CONSTANTS.NUM_PARTITION)); // OK!
		JavaRDD<String> rddreplace0 = javasparkcontext.textFile(hdfsPath + replace0);//.repartition(CONSTANTS.NUM_PARTITION); // OK!
/*		JavaPairRDD<String, String> pairrddreplace0 = rddreplace0.mapToPair(t->new Tuple2<String,String>(t, "R"));
		JavaRDD<String> result =  pairRDD_0.join(pairrddreplace0).map(new Function<Tuple2<String,Tuple2<String,Integer>>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, Tuple2<String, Integer>> t) throws Exception {
				// TODO Auto-generated method stub
				if((t._2)._2==1) return pumakey.replaceKey((t._2)._1, CONSTANTS.FIRST_COL, "R");
				return (t._2)._1;
			}
		});*/
		
		List<String> listreplace0 = rddreplace0.collect();
		Broadcast<List<String>> bc0 = javasparkcontext.broadcast(listreplace0);
/*		listreplace0 = bc0.value();
		for(String str:listreplace0) {
			System.out.println("list " +str);
		}
*/		
		JavaRDD<String> result0;
		if (col==CONST.FIRST_COL) {
				result0= pairRDD_0.map(new Function<Tuple2<String,String>, String>() {
				private static final long serialVersionUID = 1L;
	
				@Override
				public String call(Tuple2<String, String> t) throws Exception {
					// TODO Auto-generated method stub
					List<String> replace = bc0.value();
					if (replace.contains(t._1)) {
						//if(col==CONSTANTS.FIRST_COL)
							//return pumakey.replaceKey(t._2, CONSTANTS.FIRST_COL, "R");
							return pumakey.replaceKey1(t._2, CONST.FIRST_COL, "R");
						}
					else {
						//System.out.println("no replace R");
						return t._2;
					}
				}
			});
			
		}
		else {
			result0= pairRDD_0.map(new Function<Tuple2<String,String>, String>() {
				private static final long serialVersionUID = 1L;
	
				@Override
				public String call(Tuple2<String, String> t) throws Exception {
					// TODO Auto-generated method stub
					List<String> replace = bc0.value();
	//				System.out.println("key  " + t._1);
					if (replace.contains(t._1)) {
						return pumakey.replaceKey1(t._2, CONST.SECOND_COL, "S");
					}
					else {
						//System.out.println("no replace R");
						return t._2;
					}
				}
			});
			
		}
		
		result0.repartition(1).saveAsTextFile(hdfsPath+output);
		bc0.destroy();
		
		javasparkcontext.close();
	}
	
	public static void main(String[] userargs) {
		try {
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 4) {
				System.err.println("Usage:KeyReplace <path_to_dataset0> <path_to_replace0> <keycol>"
						+ "<path_to_output>");
				System.exit(2);
			}
			String inputPath0 = args[0];	
			String replace0 = args[1];	
			int col = Integer.parseInt(args[2]);
			String outputPath = args[3];

			long start = 0;
			long end = 0;
			
			keyreplace sparkProgram = new keyreplace();
			
			start = System.currentTimeMillis();
			sparkProgram.run(inputPath0, replace0, col, outputPath);
			end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

}
