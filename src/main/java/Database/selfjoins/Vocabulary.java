package Database.selfjoins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import Database.pumakey.pumakey;

import java.io.File;
import java.io.FileReader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Similarity join in Spark.
 * Vocabulary algorithm
 * 
 * @author Rémi Uhartegaray
 *
 */
public class Vocabulary {
    public static Configuration hadoopConf = new Configuration();
    public static SparkConf sparkconf;
    public static JavaSparkContext javasparkcontext;

    public static String hdfsPath = null;

    /**
     * Close the Spark context.
     */
    public void closeSparkContext() {
        Vocabulary.javasparkcontext.close();
    }

    /**
     * Create a new instance of Vocabulary.
     *
     * @throws ClassNotFoundException
     */
    public Vocabulary() throws ClassNotFoundException {
        sparkconf = new SparkConf().setAppName("Vocabulary");
        javasparkcontext = new JavaSparkContext(sparkconf);
        // hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
        hdfsPath = "";
    }

    /**
     * Create JavaPairRDD from JavaRDD.
     * 
     * @param vocabularyPath : The vocabulary path
     * @param vocabulary     : The vocabulary
     * @return : True/False : Vocabulary file created/already exists
     */
    public static boolean createVocabularyFile(String vocabularyPath, ArrayList<String> vocabulary) {

        try {
            File file = new File(vocabularyPath);
            if (file.createNewFile()) {
                System.out.println("Vocabulary file created at location: " + file.getCanonicalPath());
                Files.write(file.toPath(), vocabulary);
                return true;
            } else {
                System.err.println("Vocabulary file already exists, aborting...\n");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Create vocabulary from a string
     * 
     * @param str          : The string to create the vocabulary
     * @param key_position : The key position
     * @return : The vocabulary of the string
     */
    public static ArrayList<String> createVocabulary(String str, Integer key_position) {

        ArrayList<String> voc = new ArrayList<>();

        if (pumakey.isKeyPositionPossible(str, key_position)) { // Key position is valid
            String key = pumakey.getRecordKey(str, key_position);

            if ((key != null) && (key.length() != 0)) { // Key is valid
                char[] tmpstr = key.toCharArray();
                for (char c : tmpstr) {
                    if (!voc.contains(String.valueOf(c))) {
                        voc.add(String.valueOf(c));
                    }
                }
            }
        }
        Collections.sort(voc);
        return voc;
    }

    /**
     * Get the vocabulary of a file
     * 
     * @param vocabularyPath : The vocabulary path
     * @return : The vocabulary of the file
     * @throws FileNotFoundException
     */
    public ArrayList<String> getVocabulary(String vocabularyPath) throws FileNotFoundException {
        ArrayList<String> voc = new ArrayList<>();
        File file = new File(vocabularyPath);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String st;
        try {
            while ((st = br.readLine()) != null) {
                voc.add(st);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Vocabulary : " + voc);
        return voc;
    }

    /**
     * Concatenate two vocabularies without duplicates
     * 
     * @param voc1 : The first vocabulary
     * @param voc2 : The second vocabulary
     * @return : The unified vocabulary
     */
    public static ArrayList<String> unifyVocabularies(ArrayList<String> voc1, ArrayList<String> voc2) {

        voc1.addAll(voc2); // Concatenate the two vocabularies

        ArrayList<String> voc = new ArrayList<>();
        for (String temp_voc : voc1) {
            if (!voc.contains(temp_voc)) { // Remove duplicates
                voc.add(temp_voc);
            }
        }
        Collections.sort(voc);
        return voc;
    }

    /**
     * Run the vocabulary algorithm
     * 
     * @param inputPath      : The input path
     * @param vocabularyPath : The vocabulary path
     * @param key_position   : The key position
     */
    public void run(String datasetPath, String vocabularyPath, Integer key_position) {
        JavaRDD<String> dataset = javasparkcontext.textFile(hdfsPath + datasetPath);

        JavaRDD<ArrayList<String>> tmp_voc = dataset.map(s -> createVocabulary(s, key_position));
        ArrayList<String> vocabulary = tmp_voc.reduce((a, b) -> unifyVocabularies(a, b));

        createVocabularyFile(vocabularyPath, vocabulary);

        javasparkcontext.close();
    }

    /**
     * Main function
     * 
     * @param userarg_0 : path_to_dataset    : The dataset path
     * @param userarg_1 : path_to_vocabulary : The vocabulary path
     * @param userarg_2 : key_position       : The key position
     */
    public static void main(String[] userargs) {
        try {
            System.out.println("\n\nSTART VOCABULARY\n\n");

            // region Arguments
            String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
            if (args.length < 3) {
                System.err.println("Usage: Databse.selfjoins.Vocabulary <path_to_dataset> "
                        + "<path_to_vocabulary> <key_position>");
                System.exit(2);
            }

            String datasetPath = args[0];    // Input dataset
            String vocabularyPath = args[1]; // Vocabulary file
            int key_position = Integer.parseInt(args[2]); // Key position

            if ((!Files.exists(Paths.get(datasetPath)))) {
                System.err.print("Dataset file does not exists, aborting...\n");
                System.exit(2);
            } else if ((Files.exists(Paths.get(vocabularyPath)))) {
                System.err.println("Vocabulary file already exists, aborting...\n");
                System.exit(2);
            } else if (key_position < 0) {
                System.err.println("Key position must be positive, aborting...\n");
                System.exit(2);
            }
            // endregion
            long start = System.currentTimeMillis();

            Vocabulary vocProgram = new Vocabulary();
            vocProgram.run(datasetPath, vocabularyPath, key_position);
            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + " ms");
        } catch (

        Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
