package JSON.selfjoins;

import JSON.pumakey;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
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
 * Naive algorithm
 *
 * @author tttquyen, Rémi Uhartegaray
 */
public class Naive implements Serializable {
    public static Configuration hadoopConf = new Configuration();
    public static SparkConf sparkconf;
    public static JavaSparkContext javasparkcontext;

    public static String hdfsPath = null;

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
        sparkconf = new SparkConf().setAppName("Naive JSON fuzzy self join");
        javasparkcontext = new JavaSparkContext(sparkconf);
        // hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
        hdfsPath = "";

    }

    /**
     * Create JavaPairRDD from JavaRDD.
     * 
     * @param dataset      : The input dataset
     * @param key_position : The position of the key
     * @param maxKeyLength : The maximum key length of the dataset
     * @return : The pairs of the dataset
     * @throws NullPointerException
     */
    public static JavaPairRDD<String, String> createJavaPairRDD(JavaRDD<String> dataset, int key_position,
            int maxKeyLength)
            throws NullPointerException {
        return dataset.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

            @Override
            public Iterator<Tuple2<String, String>> call(String t) throws Exception {

                // Create an empty list of pairs : <key, value>
                ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();

                if (pumakey.isKeyPositionPossible(t, key_position)) { // Key position is valid
                    String key = pumakey.getRecordKey(t, key_position);

                    if ((key != null) && (key.length() != 0)) { // Key is valid

                        // Make like a matrix whithout the lower triangle part
                        int hashValue = pumakey.getHashCode(key.toCharArray(), maxKeyLength);
                        for (int i = hashValue; i < maxKeyLength; i++) {
                            out.add(new Tuple2<String, String>(hashValue + "_" + i, t));
                        }

                        for (int i = 0; i < hashValue; i++) {
                            out.add(new Tuple2<String, String>(i + "_" + hashValue, t));
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

                                ArrayList<String> slist = new ArrayList<String>();
                                ArrayList<String> tlist = new ArrayList<String>();

                                String skey, bkey;
                                for (String str : t._2) {
                                    slist.add(str);
                                    tlist.add(str);
                                }
                                for (String s : slist) {
                                    skey = pumakey.getRecordKey(s, 0);
                                    if ((!skey.equals("null")) && (skey.length() > 0)) {
                                        for (String b : tlist) {
                                            bkey = pumakey.getRecordKey(b, 0);
                                            if ((skey.equals(bkey)) || ((!bkey.equals("null")) && (bkey.length() > 0)
                                                    && pumakey.isSimilair(skey, bkey, eps))) {
                                                out.add(new Tuple2<String, String>(s, b));
                                            }
                                        }
                                        tlist.remove(0);
                                    }
                                }
                                return out.iterator();
                            }
                        });

        // Sort the result to avoid duplicates and add swap combinations
        ArrayList<Tuple2<String, String>> resultSort = new ArrayList<Tuple2<String, String>>();
        Tuple2<String, String> temp;
        for (Tuple2<String, String> t : joinResult.collect()) {
            temp = new Tuple2<String, String>(t._2, t._1); // Swap the tuple to avoid swap duplicates
            if (!resultSort.contains(t)) // Remove duplicates
                resultSort.add(t);
            if (!resultSort.contains(temp)) // Remove swap duplicates
                resultSort.add(temp);
        }
        joinResult = javasparkcontext.parallelizePairs(resultSort); // Copy arraylist to a RDD
        joinResult.saveAsTextFile(hdfsPath + outputPath);
    }

    /**
     * Start the Naive algorithm.
     * 
     * @param inputPath    : Input path
     * @param outputPath   : Output path
     * @param key_position : Key position
     * @param eps          : Distance threshold
     */
    public void run(String inputPath, String outputPath, int key_position, int eps) {

        JavaRDD<String> dataset = javasparkcontext.textFile(hdfsPath + inputPath);
        int maxKeyLength = pumakey.getMaxKeyLength(key_position, dataset);

        JavaPairRDD<String, String> pairRDD = createJavaPairRDD(dataset, key_position, maxKeyLength);

        fuzzyJoin(pairRDD, outputPath, eps);

        javasparkcontext.close();
    }

    /**
     * Main function.
     * 
     * @param userarg_0 : path_to_dataset : The dataset path
     * @param userarg_1 : path_to_output : The output path
     * @param userarg_2 : key_position : The key position
     * @param userarg_3 : distance_thresold : The distance threshold
     */
    public static void main(String[] userargs) {
        
        try {
            System.out.println("\n\nSTART JSON SELFJOIN NAIVE...\n\n");

            // region Arguments
            String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
            if (args.length < 4) {
                System.err.println("Usage: JSON.selfjoins.Naive <path_to_dataset> "
                        + "<path_to_output> <key_position> <distance_thresold>");
                System.exit(2);
            }

            String inputPath = args[0]; // Input dataset
            String outputPath = args[1]; // Output dataset
            int key_position = Integer.parseInt(args[2]); // Key position
            int eps = Integer.parseInt(args[3]); // Distance threshold

            /*if ((!Files.exists(Paths.get(inputPath)))) { // Check if file exists
                System.err.print("Dataset file does not exist, aborting...\n");
                System.exit(2);
            } else*/ if (Files.exists(Paths.get(outputPath))) { // Check if file exists
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

            sparkProgram.run(inputPath, outputPath, key_position, eps);
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + " ms");

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}