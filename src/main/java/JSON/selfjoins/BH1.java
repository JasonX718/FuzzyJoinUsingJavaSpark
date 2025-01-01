package JSON.selfjoins;
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
        sparkconf = new SparkConf().setAppName("BH1 JSON fuzzy self join");
        javasparkcontext = new JavaSparkContext(sparkconf);
        // hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
        hdfsPath = "";
    }

    /**
     * Create JavaBHPairRDD from JavaRDD.
     * 
     * @param dataset      : The input dataset
     * @param key_position : The key position
     * @param eps          : The threshold distance
     * @param vocabulary   : The vocabulary
     * @return : The pairs of the dataset
     * @throws NullPointerException
     */
    public static JavaPairRDD<String, Tuple2<String, String>> createBHJavaPairRDD(JavaRDD<String> dataset,
            ArrayList<String> vocabulary, int key_position, int eps, int maxKeyLength, String strEqLength)
            throws NullPointerException {
        return dataset.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String, String>>() {

            @Override
            public Iterator<Tuple2<String, Tuple2<String, String>>> call(String t) throws Exception {

                // Create an empty list of pairs : <Key, <Key, Value>>
                ArrayList<Tuple2<String, Tuple2<String, String>>> out = new ArrayList<Tuple2<String, Tuple2<String, String>>>();

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
                            pumakey.generateBHBallTuple(key_Array, key_Array, key_Array.length - 1, tmp_eps, out,
                                    t, key, vocabulary);
                            out.add(new Tuple2<String, Tuple2<String, String>>(key,
                                    new Tuple2<String, String>("-1", t)));
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
     * @param pairRDD    : The input dataset
     * @param outputPath : The output path
     * @param eps        : The threshold distance
     */
    public void fuzzyJoin(JavaPairRDD<String, Tuple2<String, String>> pairRDD, String outputPath, int eps) {
        JavaPairRDD<String, String> joinResult = pairRDD.groupByKey().flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, String>() {
                    private static final long serialVersionUID = 1L;

                    public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<Tuple2<String, String>>> t)
                            throws Exception {
                        ArrayList<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();

                        ArrayList<String> s1list = new ArrayList<String>();
                        ArrayList<String> s2list = new ArrayList<String>();
                        ArrayList<String> blist = new ArrayList<String>();

                        for (Tuple2<String, String> p : t._2) {
                            if (p._1.compareTo("-1") == 0) {
                                s1list.add(p._2);
                            } else
                                blist.add(p._2);
                        }
                        String skey, bkey;
                        s2list.addAll(s1list);

                        for (String s : s1list) {
                            skey = pumakey.getRecordKey(s, 0);
                            if (s2list.size() > 0) {
                                for (String b : s2list) {
                                    bkey = pumakey.getRecordKey(b, 0);
                                    if (!(out.contains(new Tuple2<String, String>(s, b)))
                                            && (pumakey.isSimilair(skey, bkey, eps)))
                                        out.add(new Tuple2<String, String>(s, b));
                                }
                            }
                            for (String b : blist) {
                                bkey = pumakey.getRecordKey(b, 0);
                                if (!(out.contains(new Tuple2<String, String>(s, b)))
                                        && (pumakey.isSimilair(skey, bkey, eps)))
                                    out.add(new Tuple2<String, String>(s, b));
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
     * Start the BH1 algorithm
     * 
     * @param inputPath    : The input path
     * @param outputPath   : The output path
     * @param key_position : The key position
     * @param eps          : The threshold distance
     * @param vocabulary   : The vocabulary
     */
    public void run(String inputPath, String outputPath, int key_position, int eps,
            ArrayList<String> vocabulary) {

        JavaRDD<String> dataset = javasparkcontext.textFile(hdfsPath + inputPath);
        int maxKeyLength = pumakey.getMaxKeyLength(key_position, dataset);
        String strEqLength = pumakey.getAdditionalChar(vocabulary);

        JavaPairRDD<String, Tuple2<String, String>> pairRDD = createBHJavaPairRDD(dataset, vocabulary, key_position,
                eps, maxKeyLength, strEqLength);

        fuzzyJoin(pairRDD, outputPath, eps);

        javasparkcontext.close();
    }

    /**
     * Main function
     * 
     * @param userarg_0 : path_to_dataset : The dataset path
     * @param userarg_1 : path_to_output : The output path
     * @param userarg_2 : path_to_vocabulary : The vocabulary path
     * @param userarg_3 : key_position : The key position
     * @param userarg_4 : distance_thresold : The threshold distance
     * @param userarg_5 : vocabulary_status : The vocabulary status
     */
    public static void main(String[] userargs) {
        try {
            System.out.println("\n\nSTART JSON SELFJOIN BH1...\n\n");

            // region Arguments
            String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
            if (args.length < 6) {
                System.err.println("Usage: JSON.selfjoins.BH1 <path_to_dataset> "
                        + "<path_to_output> <path_to_vocabulary> <key_position> <distance_thresold> <vocabulary_status>");
                System.exit(2);
            }

            String inputPath = args[0]; // Dataset path
            String outputPath = args[1]; // Output dataset
            String vocabularyPath = args[2]; // Vocabulary path
            int key_position = Integer.parseInt(args[3]); // Key position
            int eps = Integer.parseInt(args[4]); // Distance threshold
            int voc_status = Integer.parseInt(args[5]); // 0/1 : create/load vocabulary

            if ((!Files.exists(Paths.get(inputPath)))) {
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
                vocProgram.run(inputPath, vocabularyPath, key_position);

            ArrayList<String> vocabulary = vocProgram.getVocabulary(vocabularyPath);
            vocProgram.closeSparkContext();

            BH1 sparkProgram = new BH1();

            sparkProgram.run(inputPath, outputPath, key_position, eps, vocabulary);
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + " ms");
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
