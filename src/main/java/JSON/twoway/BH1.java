package JSON.twoway;
import JSON.pumakey;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * BH1 algorithm
 * 
 * @author tttquyen, Rémi Uhartegaray
 *
 */
public class BH1 implements Serializable {
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
		sparkconf = new SparkConf().setAppName("BH1 JSON fuzzy twoway join");
		javasparkcontext = new JavaSparkContext(sparkconf);
		// hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
		hdfsPath = "";
	}

	/**
	 * Create JavaBHPairRDD from JavaRDD.
	 * whithout adding one for eps to the keys that are shorter than the longest key
	 * 
	 * @param dataset      : The input dataset
	 * @param vocabulary   : The vocabulary
	 * @param key_position : The key position
	 * @param eps          : The threshold distance
	 * @param maxKeyLength : The maximum key length of the datasets
 	 * @param strEqLength  : The character that's not in the vocabulary and used to equalize the key's length
	 * @return : The pairs of the dataset
	 * @throws NullPointerException
	 */
	public static JavaPairRDD<String, String> createJavaPairRDD(JavaRDD<String> dataset, ArrayList<String> vocabulary, int key_position, int eps,
			int maxKeyLength, String strEqLength) throws NullPointerException {
		return dataset.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {

				ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
				if (pumakey.isKeyPositionPossible(t, key_position)) { // Key position is valid
					String key = pumakey.getRecordKey(t, key_position);

					if (key.length() < maxKeyLength) {
						while (key.length() < maxKeyLength) {
							key += strEqLength;
						}
						vocabulary.add(strEqLength);
						Collections.sort(vocabulary);
					}
					if ((key != null) && (key.length() != 0)) { // Key is valid
						try {
							char[] key_Array = key.toCharArray();
							pumakey.generateBHBall2way(key_Array, key_Array, key_Array.length - 1, eps, out, t,
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
	 * Create JavaBHPairRDD from JavaRDD.
	 * 
	 * @param dataset      : The input dataset
	 * @param vocabulary   : The vocabulary
	 * @param key_position : The key position
	 * @param eps          : The threshold distance
	 * @param maxKeyLength : The maximum key length of the datasets
	 * @param strEqLength  : The character that's not in the vocabulary and used to equalize the key's length
	 * @return : The pairs of the dataset
	 * @throws NullPointerException
	 */
	public static JavaPairRDD<String, String> createBHJavaPairRDD(JavaRDD<String> dataset, ArrayList<String> vocabulary, int key_position, int eps,
			 int maxKeyLength, String strEqLength)
			throws NullPointerException {
		return dataset.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {

				ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
				if (pumakey.isKeyPositionPossible(t, key_position)) { // Key position is valid
					String key = pumakey.getRecordKey(t, key_position);

					int tmp_eps = eps; // if (key.length < 10) : eps+1
					if (key.length() < maxKeyLength) {
						while (key.length() < maxKeyLength) {
							key += strEqLength;
						}
						tmp_eps += 1;
						vocabulary.add(strEqLength);
						Collections.sort(vocabulary);
					}
					if ((key != null) && (key.length() != 0)) { // Key is valid
						try {
							char[] key_Array = key.toCharArray();
							pumakey.generateBHBall2way(key_Array, key_Array, key_Array.length - 1, tmp_eps, out, t,
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
	 * @param eps        : The threshold distance
	 */
	public void fuzzyJoin(JavaPairRDD<String, String> pairRDD_1, JavaPairRDD<String, String> pairRDD_2,
			String outputPath, int eps) {
		
		JavaPairRDD<String, String> joinResult = pairRDD_1.join(pairRDD_2).mapToPair(t -> t._2);

		// Sort the result to avoid duplicates and add swap combinations
		ArrayList<Tuple2<String, String>> resultSort = new ArrayList<Tuple2<String, String>>();
		Tuple2<String, String> temp;
		String key1, key2;
		for (Tuple2<String, String> t : joinResult.collect()) {
			key1 = pumakey.getRecordKey(t._1, 0);
			key2 = pumakey.getRecordKey(t._2, 0);
			temp = new Tuple2<String, String>(t._2, t._1);
			if (pumakey.isSimilair(key1, key2, eps)) { // Check if the records are similair
				if (!resultSort.contains(t)) // Remove duplicates
					resultSort.add(t);
				if (!resultSort.contains(temp)) // Remove swap duplicates
					resultSort.add(temp);
			}
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

		int maxKeyLength = pumakey.getMaxKeyLength(key_position, dataset_1, dataset_2);
		String strEqLength = pumakey.getAdditionalChar(vocabulary);

		JavaPairRDD<String, String> pairRDD_1 = createJavaPairRDD(dataset_1, vocabulary, key_position, eps, 
				maxKeyLength, strEqLength);
		JavaPairRDD<String, String> pairRDD_2 = createBHJavaPairRDD(dataset_2, vocabulary, key_position, eps,
				maxKeyLength, strEqLength);
		
		
		System.out.println("fuzzyjoin start");
		fuzzyJoin(pairRDD_1, pairRDD_2, outputPath, eps);

		javasparkcontext.close();
	}

	/**ut
	 * Main function
	 * 
	 * @param userarg_0 : path_to_dataset_1 : The first dataset path
	 * @param userarg_1 : path_to_dataset_2 : The second dataset path
	 * @param userarg_2 : path_to_output : The output path
	 * @param userarg_3 : path_to_vocabulary : The vocabulary path
	 * @param userarg_4 : key_position : The key position
	 * @param userarg_5 : distance_thresold : The threshold distance
	 * @param userarg_6 : vocabulary_status : The vocabulary status
	 */
	public static void main(String[] userargs) {
		try {
			System.out.println("\n\nSTART JSON TWOWAY BH1...\n\n");

			// region Arguments
			String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
			if (args.length < 7) {
				System.err.println("Usage: JSON.twoway.BH1 <path_to_dataset_1> <path_to_dataset_2> "
						+ "<path_to_output> <path_to_vocabulary> <key_position> <distance_thresold> <vocabulary_status>");
				System.exit(2);
			}
			String datasetPath_1 = args[0]; // First Dataset path
			String datasetPath_2 = args[1]; // Second Dataset path
			String outputPath = args[2]; // Output dataset
			String vocabularyPath = args[3]; // Vocabulary path
			int key_position = Integer.parseInt(args[4]); // Key position
			int eps = Integer.parseInt(args[5]); // Distance threshold
			int voc_status = Integer.parseInt(args[6]); // 0/1 : create/load vocabulary

			if ((!Files.exists(Paths.get(datasetPath_1)))) {
				System.err.print("Dataset file does not exists, aborting...\n");
				System.exit(2);
			} else if ((!Files.exists(Paths.get(datasetPath_2)))) {
				System.err.print("Dataset file does not exists, aborting...\n");
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
