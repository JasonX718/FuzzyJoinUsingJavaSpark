package Database.twoway;

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
		sparkconf = new SparkConf().setAppName("Splitting fuzzy twoway join");
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
	 * Fuzzy join two datasets.
	 * 
	 * @param pairRDD_1  : The first dataset
	 * @param pairRDD_2  : The second dataset
	 * @param outputPath : The output path
	 */
	public void fuzzyJoin(JavaPairRDD<String, String> pairRDD_1, JavaPairRDD<String, String> pairRDD_2,
			String outputPath) {

		JavaPairRDD<String, String> joinResult = pairRDD_1.join(pairRDD_2)
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {

					@Override
					public Iterator<Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> t)
							throws Exception {
						ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
						String keyList1 = pumakey.getRecordKey(t._2._1, key_position);
						String keyList2 = pumakey.getRecordKey(t._2._2, key_position);
						if ((keyList1.length() != 0) && (keyList2.length() != 0)
								&& pumakey.isSimilair(keyList1, keyList2, eps)) {
							out.add(new Tuple2<String, String>(t._2._1, t._2._2));
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
	 * Start the twoway Splitting algorithm.
	 * 
	 * @param inputPath_1  : The first input path
	 * @param inputPath_2  : The second input path
	 * @param outputPath   : the output path
	 */
	public void run(String datasetPath_1, String datasetPath_2, String outputPath) {

		JavaRDD<String> dataset_1 = javasparkcontext.textFile(hdfsPath + datasetPath_1);
		JavaRDD<String> dataset_2 = javasparkcontext.textFile(hdfsPath + datasetPath_2);

		JavaPairRDD<String, String> pairRDD_1 = createSplitsJavaPairRDD(dataset_1);
		JavaPairRDD<String, String> pairRDD_2 = createSplitsJavaPairRDD(dataset_2);

		fuzzyJoin(pairRDD_1, pairRDD_2, outputPath);

		javasparkcontext.close();
	}

	/**
	 * Main function.
	 * 
	 * @param userarg_0 : path_to_dataset_1 : The first input dataset
	 * @param userarg_1 : path_to_dataset_2 : The second input dataset
	 * @param userarg_2 : path_to_output 	: The output dataset
	 * @param userarg_3 : key_position		: The key position
	 * @param userarg_4 : eps 				: The threshold distance
	 */
	public static void main(String[] userargs) {
		try {
			System.out.println("\n\nSTART TWOWAY SPLITTING...\n\n");

			// region Arguments
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 5) {
				System.err.println("Usage: Database.twoway.Splitting <path_to_dataset_1> <path_to_dataset_2> "
						+ "<path_to_output> <key_position> <eps>");
				System.exit(2);
			}
			String datasetPath_1 = args[0]; 		  // First input path
			String datasetPath_2 = args[1]; 		  // Second input path
			String outputPath = args[2]; 			  // Output path
			key_position = Integer.parseInt(args[3]); // Key position
			eps = Integer.parseInt(args[4]); 		  // Threshold distance

			if ((!Files.exists(Paths.get(datasetPath_1)))) {
				System.err.print("Dataset file 1 does not exist, aborting...\n");
				System.exit(2);
			} else if ((!Files.exists(Paths.get(datasetPath_2)))) {
				System.err.print("Dataset file 2 does not exist, aborting...\n");
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
			Splitting sparkProgram = new Splitting();

			sparkProgram.run(datasetPath_1, datasetPath_2, outputPath);
			long end = System.currentTimeMillis();
			System.out.println("running time " + (end - start) / 1000 + " s");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}