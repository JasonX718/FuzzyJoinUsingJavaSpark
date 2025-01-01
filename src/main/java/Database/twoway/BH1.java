package Database.twoway;

import java.io.Serializable;
import java.util.Iterator;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import Database.pumakey.pumakey;
import filters.BallOfRadius;
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * BH1 algorithm
 * 
 * @author tttquyen, Rémi Uhartegaray
 *
 */

public class BH1 implements Serializable {
	private static final long serialVersionUID = 1L;
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;

	public static String hdfsPath = null;

	/**
	 * Close the Spark context.
	 */
	public void closeSparkContext() {
		BH1.javasparkcontext.close();
	}

	/**
	 * Create a new instance of BH1.
	 * 
	 * @throws ClassNotFoundException
	 */
	public BH1() throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("BH1 fuzzy twoway join");
		javasparkcontext = new JavaSparkContext(sparkconf);
		// hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
		hdfsPath = "";
	}

	
    /**
     * Create JavaPairRDD from JavaRDD.
     * 
     * @param dataset : The input dataset
	 * @param key_position : The key position
     * @return : The pairs of the dataset
     * @throws NullPointerException
     */
	public static JavaPairRDD<String, String> createJavaPairRDD(JavaRDD<String> dataset, int key_position)
			throws NullPointerException {
		return dataset.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {

				ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
				String key = pumakey.getRecordKey(t, key_position);
				if ((key != null) && (key.length() != 0)) { // Key is valid
					out.add(new Tuple2<String, String>(key, t));
				}
				return out.iterator();
			}
		});
	}


    /**
     * Create JavaBHPairRDD from JavaRDD.
     * 
     * @param dataset        : The input dataset
     * @param key_position   : The key position
     * @param eps            : The threshold distance
     * @param vocabulary     : The vocabulary
     * @return : The pairs of the dataset
     * @throws NullPointerException
     */
	public static JavaPairRDD<String, String> createBHJavaPairRDD(JavaRDD<String> dataset, int key_position, int eps,
			ArrayList<String> vocabulary)
			throws NullPointerException {
		return dataset.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {

				ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
				if (pumakey.isKeyPositionPossible(t, key_position)) { // Key position is valid
					String key = pumakey.getRecordKey(t, key_position);

					if ((key != null) && (key.length() != 0)) { // Key is valid
						try {
							char[] key_Array = key.toCharArray();
							BallOfRadius.generateBHBall2way(key_Array, key_Array, key_Array.length - 1, eps, out, t,
									key, vocabulary);
							out.add(new Tuple2<String, String>(key, t));
						} catch (Exception e) {
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
	 * @param pairRDD_1  : The first input dataset
	 * @param pairRDD_2  : The second input dataset
	 * @param outputPath : The output path
	 */
	public void fuzzyJoin(JavaPairRDD<String, String> pairRDD_1, JavaPairRDD<String, String> pairRDD_2,
			String outputPath) {

		JavaPairRDD<String, String> joinResult = pairRDD_1.join(pairRDD_2).mapToPair(t -> t._2);

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
	 * Start the twoway BH1 algorithm.
	 * 
	 * @param inputPath_1  : The first input path
	 * @param inputPath_2  : The second input path
	 * @param outputPath   : the output path
	 * @param key_position : The key position
	 * @param eps          : The threshold distance
	 * @param vocabulary   : The vocabulary
	 */
	public void run(String inputPath_1, String inputPath_2, String outputPath, int key_position, int eps,
			ArrayList<String> vocabulary) {

		JavaRDD<String> dataset_1 = javasparkcontext.textFile(hdfsPath + inputPath_1);
		JavaRDD<String> dataset_2 = javasparkcontext.textFile(hdfsPath + inputPath_2);

		JavaPairRDD<String, String> pairRDD_1 = createJavaPairRDD(dataset_1, key_position);
		JavaPairRDD<String, String> pairRDD_2 = createBHJavaPairRDD(dataset_2, key_position, eps, vocabulary);

		fuzzyJoin(pairRDD_1, pairRDD_2, outputPath);

		javasparkcontext.close();
	}

	/**
	 * Main function
	 * 
	 * @param userarg_0 : path_to_dataset_1  : The first dataset path
	 * @param userarg_1 : path_to_dataset_2  : The second dataset path
	 * @param userarg_2 : path_to_output 	 : The output path
	 * @param userarg_3 : path_to_vocabulary : The vocabulary path
	 * @param userarg_4 : key_position  	 : The key position
	 * @param userarg_5 : distance_thresold  : The threshold distance
	 * @param userarg_6 : vocabulary_status  : The vocabulary status
	 */
	public static void main(String[] userargs) {
		try {
			System.out.println("\n\nSTART TWOWAY BH1...\n\n");

			// region Arguments
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 7) {
				System.err.println("Usage: Database.twoway.BH1 <path_to_dataset_1> <path_to_dataset_2> "
						+ "<path_to_output> <path_to_vocabulary> <key_position> <distance_thresold> <vocabulary_status>");
				System.exit(2);
			}
			String datasetPath_1 = args[0];  // First Dataset path
			String datasetPath_2 = args[1];  // Second Dataset path
			String outputPath = args[2]; 	 // Output dataset
			String vocabularyPath = args[3]; // Vocabulary path
			int key_position = Integer.parseInt(args[4]); // Key position
			int eps = Integer.parseInt(args[5]); 		  // Distance threshold
			int voc_status = Integer.parseInt(args[6]);   // 0/1 : create/load vocabulary

			if ((!Files.exists(Paths.get(datasetPath_1)))) {
				System.err.print("Dataset file 1 does not exists, aborting...\n");
				System.exit(2);
			} else if ((!Files.exists(Paths.get(datasetPath_2)))) {
				System.err.print("Dataset file 2 does not exists, aborting...\n");
				System.exit(2);
			} else if ((Files.exists(Paths.get(outputPath)))) {
				System.err.println("Output folder already exists, aborting...\n");
				System.exit(2);
			} else if (key_position < 0) {
				System.err.print("Key position must be positive, aborting...\n");
				System.exit(2);
			} else if (eps < 0) {
				System.err.print("Distance thresold must be positive, aborting...\n");
				System.exit(2);
			} else if ((voc_status != 1) && (voc_status != 0)) {
				System.err.println("voc_status = " + voc_status + " is not valid, aborting...\n");
				System.exit(2);

			} else if (voc_status == 0) {
				if ((Files.exists(Paths.get(vocabularyPath)))) {
					System.err.print("Vocabulary file already exists, aborting...\n");
					System.exit(2);
				}
			}
			// endregion
			long start = System.currentTimeMillis();

			Vocabulary vocProgram = new Vocabulary();
			if (voc_status == 0) // Create vocabulary
				vocProgram.run(datasetPath_1, datasetPath_2, vocabularyPath, key_position);

			ArrayList<String> vocabulary = vocProgram.getVocabulary(vocabularyPath);
			vocProgram.closeSparkContext();

			BH1 sparkProgram = new BH1();

			sparkProgram.run(datasetPath_1, datasetPath_2, outputPath, key_position, eps, vocabulary);
			long end = System.currentTimeMillis();
			System.out.println("Time: " + (end - start) + " ms");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
