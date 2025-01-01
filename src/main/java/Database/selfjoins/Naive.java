package Database.selfjoins;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

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
 * Naive algorithm
 *
 * @author tttquyen, Rémi Uhartegaray
 */
public class Naive implements Serializable {
    public static Configuration hadoopConf = new Configuration();
    public static SparkConf sparkconf;
    public static JavaSparkContext javasparkcontext;

    public static String hdfsPath = null;
    public static int key_position = 0;

    /**
     * Close the Spark context.
     */
    public void closeSparkContext() {
        Naive.javasparkcontext.close();
    }

    /**
     * Create a new instance of Naive.
     * 
     * @throws ClassNotFoundException
     */
    public Naive() throws ClassNotFoundException {
        sparkconf = new SparkConf().setAppName("Naive fuzzy self join");
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
    public static JavaPairRDD<String, String> createJavaPairRDD(JavaRDD<String> dataset)
            throws NullPointerException {
        return dataset.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

            @Override
            public Iterator<Tuple2<String, String>> call(String t) throws Exception {

                // Create an empty list of pairs : <key, value>
                ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();

                if (pumakey.isKeyPositionPossible(t, key_position)) { // Key position is valid
                    String key = pumakey.getRecordKey(t, key_position);

                    if ((key != null) && (key.length() != 0)) { // Key is valid

                        int keyLength = key.length();

                        // Make like a matrix whithout the lower triangle part
                        int i = pumakey.getHashCode(key.toCharArray(), keyLength);
                        for (int j = i; j < keyLength; j++) {
                            out.add(new Tuple2<String, String>(i + "_" + j, t));
                        }
                        for (int j = 0; j < i; j++) {
                            out.add(new Tuple2<String, String>(j + "_" + i, t));
                        }
                    }
                }
                return out.iterator(); // Return the list of pairs
            }
        });
    }

    /**
     * Fuzzy self join.
     * 
     * @param pairRDD    : The input dataset
     * @param outputPath : The output path
     * @param eps        : The threshold distance
     */
    public void fuzzyJoin(JavaPairRDD<String, String> pairRDD, String outputPath, int eps) {
        JavaPairRDD<String, String> joinResult = pairRDD.groupByKey()
                .flatMapToPair(
                        new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

                            @Override
                            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t)
                                    throws Exception {

                                ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
                                ArrayList<String> list1 = new ArrayList<String>();

                                list1.addAll((Collection<? extends String>) t._2());

                                if (list1.size() > 1) {

                                    list1.remove(list1.size() - 1);

                                    ArrayList<String> list2 = new ArrayList<String>();
                                    list2.addAll((Collection<? extends String>) t._2());
                                    list2.remove(0);

                                    String keyList1, keyList2; // Keys of the record
                                    for (String kvList1 : list1) {
                                        keyList1 = pumakey.getRecordKey(kvList1, key_position); // Record key 1

                                        for (String kvList2 : list2) {
                                            keyList2 = pumakey.getRecordKey(kvList2, key_position); // Record key 2
                                            // Check if there is no duplicates
                                            if (!out.contains(new Tuple2<String, String>(kvList1, kvList2))
                                                    && !out.contains(new Tuple2<String, String>(kvList2, kvList1)))
                                                if (pumakey.isSimilair(keyList1, keyList2, eps))
                                                    out.add(new Tuple2<String, String>(kvList1, kvList2)); // Store K-V
                                        }
                                        list2.remove(0); // Remove the first element of the list
                                    }
                                }
                                return out.iterator(); // Return the list of pairs
                            }
                        });

        // Sort the result to avoid duplicates
        ArrayList<Tuple2<String, String>> resultSort = new ArrayList<Tuple2<String, String>>();
        for (Tuple2<String, String> t : joinResult.collect()) {
            if (!resultSort.contains(t)) // Remove duplicates
                resultSort.add(t);
        }
        joinResult = javasparkcontext.parallelizePairs(resultSort); // Copy arraylist to a RDD

        joinResult.saveAsTextFile(hdfsPath + outputPath);
    }

    /**
     * Start the Naive algorithm.
     * 
     * @param inputPath  : Input path
     * @param outputPath : Output path
     * @param eps        : Distance threshold
     */
    public void run(String inputPath, String outputPath, int eps) {

        JavaRDD<String> dataset = javasparkcontext.textFile(hdfsPath + inputPath);

        JavaPairRDD<String, String> pairRDD = createJavaPairRDD(dataset);

        fuzzyJoin(pairRDD, outputPath, eps);

        javasparkcontext.close();
    }

    /**
     * Main function.
     * 
     * @param userarg_0 : path_to_dataset   : The dataset path
     * @param userarg_1 : path_to_output    : The output path
     * @param userarg_2 : key_position      : The key position
     * @param userarg_3 : distance_thresold : The distance threshold
     */
    public static void main(String[] userargs) {
        try {
            System.out.println("\n\nSTART SELFJOIN NAIVE...\n\n");

            // region Arguments
            String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
            if (args.length < 4) {
                System.err.println("Usage: Database.selfjoins.Naive <path_to_dataset> "
                        + "<path_to_output> <key_position> <distance_thresold>");
                System.exit(2);
            }

            String inputPath = args[0];  // Input dataset
            String outputPath = args[1]; // Output dataset
            key_position = Integer.parseInt(args[2]); // Key position
            int eps = Integer.parseInt(args[3]);      // Distance threshold

            if ((!Files.exists(Paths.get(inputPath)))) { // Check if file exists
                System.err.print("Dataset file does not exist, aborting...\n");
                System.exit(2);
            } else if (Files.exists(Paths.get(outputPath))) { // Check if file exists
                System.err.print("Output file already exists, aborting...\n");
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
            Naive sparkProgram = new Naive(); // Create a new instance of the program

            sparkProgram.run(inputPath, outputPath, eps);
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + " ms");

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}