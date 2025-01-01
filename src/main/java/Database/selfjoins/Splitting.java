package Database.selfjoins;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * Splitting algorithm
 * 
 * @author tttquyen, Rémi Uhartegaray
 *
 */

public class Splitting implements Serializable {
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;

	public static String hdfsPath = null;
	public static int key_position = 0;
	public static int eps = 0;

	/**
	 * Close the Spark context.
	 */
	public void closeSparkContext() {
		Splitting.javasparkcontext.close();
	}

	/**
	 * Create a new instance of Splitting.
	 * 
	 * @throws ClassNotFoundException
	 */
	public Splitting() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("Splitting fuzzy self join");
		javasparkcontext = new JavaSparkContext(sparkconf);
		// hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
		hdfsPath = "";
	}

	/**
	 * Create JavaPairRDD from JavaRDD.
	 * 
	 * @param dataset : The input dataset
	 * @return : The pairs of the dataset
	 * @throws NullPointerException
	 */
	public static JavaPairRDD<String, String> createSplitsJavaPairRDD(JavaRDD<String> dataset)
			throws NullPointerException {
		return dataset.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {

				ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
				if (pumakey.isKeyPositionPossible(t, key_position)) { // Key position is valid
					String key = pumakey.getRecordKey(t, key_position);
					if ((key != null) && (key.length() != 0)) { // Key is valid
						try {
							String[] tmpsplits = pumakey.getIndex_Splits(key, eps);
 							for (String tmp : tmpsplits) {
								out.add(new Tuple2<String, String>(tmp, t));
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				return out.iterator();
			}
		});
	}

	/**
	 * Fuzzy self join.
	 * 
	 * @param dataset 	 : The input dataset
	 * @param outputPath : The output path
	 * @return : The pairs of the dataset
	 * @throws NullPointerException
	 */
	public void fuzzyJoin(JavaPairRDD<String, String> dataset, String outputPath) {
		JavaPairRDD<String, String> joinResult = dataset.groupByKey()
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
						ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();

						ArrayList<String> slist = new ArrayList<String>();
						ArrayList<String> tlist = new ArrayList<String>();

						String skey, bkey;
						for (String str : t._2) {
							slist.add(str);
							tlist.add(str);
						}
						if (slist.size() > 1) {
							slist.remove(slist.size() - 1);
							tlist.remove(0);

							for (String s : slist) {
								skey = pumakey.getRecordKey(s, key_position);
								if (skey != null && skey.length() > 0) {
									for (String b : tlist) {
										bkey = pumakey.getRecordKey(b, key_position);
										if (bkey != null && bkey.length() > 0 && pumakey.isSimilair(skey, bkey, eps)) {
											out.add(new Tuple2<String, String>(s, b));
										}
									}
									tlist.remove(0);
								}
							}
						}
						return out.iterator();
					}
				});

		// Sort the result to avoid duplicates
		ArrayList<Tuple2<String, String>> resultSort = new ArrayList<Tuple2<String, String>>();
		Tuple2<String, String> temp;
		for (Tuple2<String, String> t : joinResult.collect()) {
			temp = new Tuple2<String, String>(t._2, t._1); // Swap the tuple to avoid swap duplicates
			if (!resultSort.contains(t) && !resultSort.contains(temp)) // Remove duplicates
				resultSort.add(t);
		}

		joinResult = javasparkcontext.parallelizePairs(resultSort); // Copy arraylist to a RDD

		joinResult.saveAsTextFile(hdfsPath + outputPath);
	}

	/**
	 * Start the Splitting algorithm.
	 * 
	 * @param datasetPath : The input dataset
	 * @param outputPath  : The output dataset
	 */
	public void run(String datasetPath, String outputPath) {

		JavaRDD<String> dataset = javasparkcontext.textFile(hdfsPath + datasetPath);

		JavaPairRDD<String, String> pairRDD = createSplitsJavaPairRDD(dataset);

		fuzzyJoin(pairRDD, outputPath);

		javasparkcontext.close();
	}

	/**
	 * Main function.
	 * 
	 * @param userarg_0 : path_to_dataset : The input dataset
	 * @param userarg_1 : path_to_output  : The output dataset
	 * @param userarg_2 : key_position 	  : The key position
	 * @param userarg_3 : eps 			  : The threshold distance
	 */
	public static void main(String[] userargs) {
		try {
			System.out.println("\n\nSTART SELFJOIN SPLITTING...\n\n");

			// region Arguments
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 4) {
				System.err.println("Usage: Database.selfjoins.Splitting <path_to_dataset> "
						+ "<path_to_output> <key_position> <eps>");
				System.exit(2);
			}

			String datasetPath = args[0]; // Input dataset
			String outputPath = args[1];  // Output dataset
			key_position = Integer.parseInt(args[2]); // Key position
			eps = Integer.parseInt(args[3]); 	  	  // Distance threshold

			if ((!Files.exists(Paths.get(datasetPath)))) {
				System.err.print("Dataset file does not exist, aborting...\n");
				System.exit(2);
			} else if ((Files.exists(Paths.get(outputPath)))) {
				System.err.println("Output folder already exists, aborting...\n");
				System.exit(2);
			} else if (key_position < 0) {
				System.err.print("Key position must be positive, aborting...\n");
				System.exit(2);
			} else if (eps < 0) {
				System.err.print("Distance threshold must be positive, aborting...\n");
				System.exit(2);
			}
			// endregion

			long start = System.currentTimeMillis();
			Splitting sparkProgram = new Splitting(); // Create a new instance of the program

			sparkProgram.run(datasetPath, outputPath);
			long end = System.currentTimeMillis();
			System.out.println("Time: " + (end - start) + " ms");

		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}
}