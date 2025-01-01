package JSON;
import JSON.selfjoins.BH1;
import JSON.selfjoins.JsonReqPath;
import JSON.selfjoins.Naive;
import JSON.selfjoins.Splitting;
import JSON.selfjoins.Vocabulary;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Main class to run the algorithms.
 * Selfjoins / Twoway algorithms.
 *
 * @author Rémi Uhartegaray
 */
public class Main {

    /**
     * Main function to runs the algorithms with json requests paths.
     * @remark To call the Selfjoins algorithms, path_to_dataset_2 and jsonpath_request_2 must be null.
     * @remark To call the Twoway algorithms, path_to_dataset_2 and jsonpath_request_2 must be specified.
     * @param path_to_dataset_1  : The dataset path 1 <i>(Selfjoins / Twoway)</i>
     * @param path_to_dataset_2  : The dataset path 2 <i>(Selfjoins: null / Twoway:
     *                           path_to_dataset_2)</i>
     * @param jsonpath_request_1 : The jsonpath request 1 <i>(Selfjoins /
     *                           Twoway)</i>
     * @param jsonpath_request_2 : The jsonpath request 2 <i>(Selfjoins: null /
     *                           Twoway: jsonpath_request_2)</i>
     * @param path_to_output     : The output path <i>(Selfjoins / Twoway)</i>
     * @param path_to_vocabulary : The vocabulary path <i>(BH1, Vocabulary:
     *                           path_to_vocabulary / others: null)</i>
     * @param algorithm          : The algorithm name <i>(Naive, BH1, JsonReqPath,
     *                           Splitting, Vocabulary)</i>
     * @param distance_thresold  : The threshold distance <i>(Vocabulary: -1 /
     *                           others: 0 < distance_thresold)</i>
     * @param vocabulary_status  : The vocabulary status <i>(BH1: 0/1: create/load
     *                           existing vocabulary / others: -1: do not use
     *                           vocabulary)</i>
     */
    public static void main(String[] userargs) {
        try {
            System.out.println("\n\nSTART JSON Main...\n\n");

            // region get arguments
            String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
            
            System.out.print("parameters: ");
                for (String arg : args) {
                    System.out.print(arg + " ");
                }
            System.out.println();

            //if (false && args.length < 9) {
            if (args.length < 9) {
                System.err.println("Usage: JSON.Main <path_to_dataset_1> "
                        + "<path_to_dataset_2> <jsonpath_request_1> <jsonpath_request_2> <path_to_output> <path_to_vocabulary> <algorithm> <distance_thresold> <vocabulary_status>");
                System.exit(2);
            } else {
                String inputPath_1 = args[0]; // Dataset path 1
                String inputPath_2 = args[1]; // Dataset path 2
                String jsonpath_request_1 = args[2]; // Dataset path 1
                String jsonpath_request_2 = args[3]; // Dataset path 2
                String outputPath = args[4]; // Output dataset
                String vocabularyPath = args[5]; // Vocabulary path
                String algo = args[6]; // Algorithm name
                int eps = Integer.parseInt(args[7]); // Distance threshold
                int voc_status = Integer.parseInt(args[8]); // Vocabulary status
                // endregion get arguments

                /*
                 * //Print arguments
                 * 
                 * System.out.println("inputPath_1: " + inputPath_1);
                 * System.out.println("inputPath_2: " + inputPath_2);
                 * System.out.println("jsonpath_request_1: " + jsonpath_request_1);
                 * System.out.println("jsonpath_request_2: " + jsonpath_request_2);
                 * System.out.println("outputPath: " + outputPath);
                 * System.out.println("vocabularyPath: " + vocabularyPath);
                 * System.out.println("algo: " + algo);
                 * System.out.println("eps: " + eps);
                 * System.out.println("voc_status: " + voc_status);
                 */

                outputPath = outputPath + algo;

                // region basic checks arguments
                if ((!Files.exists(Paths.get(inputPath_1))) || (inputPath_1.equals("null"))) {
                    System.err.print("Input file 1 does not exist, aborting...\n");
                    System.exit(2);
                } else if ((!Files.exists(Paths.get(inputPath_2))) && (!inputPath_2.equals("null"))) {
                    System.err.print("Input file 2 does not exist, aborting...\n");
                    System.exit(2);
                } else if ((Files.exists(Paths.get(outputPath))) || (outputPath.equals("null"))) {
                    System.err.print("Output file already exists, aborting...\n");
                    System.exit(2);
                } else if (jsonpath_request_1.equals("null")) {
                    System.err.print("jsonpath_request_1 is not valid, aborting...\n");
                    System.exit(2);
                } else if (((jsonpath_request_2.equals("null") ^ inputPath_2.equals("null")))) {
                    System.err
                            .print("Specify <path_to_dataset_2> and <jsonpath_request_2> or leave them both null, aborting...\n");
                    System.exit(2);
                } else if (((voc_status != 0) && (voc_status != 1)) && (algo.equals("BH1"))) {
                    System.err.println("voc_status = " + voc_status + " is not valid for BH1, aborting...\n");
                    System.exit(2);
                } else if (((voc_status == 0) || (voc_status == 1)) && !algo.equals("BH1")) {
                    System.err.println("voc_status = " + voc_status + " is not valid for " + algo + ", aborting...\n");
                    System.exit(2);
                } else if ((voc_status == 0) && (Files.exists(Paths.get(vocabularyPath)))) {
                    System.err.print("Vocabulary file already exists (voc_status = 0), aborting...\n");
                    System.exit(2);
                } else if ((voc_status == 1) && (!Files.exists(Paths.get(vocabularyPath)))) {
                    System.err.print("Vocabulary file does not exists (voc_status = 1), aborting...\n");
                    System.exit(2);
                } else if ((!vocabularyPath.equals("null"))
                        && ((!algo.equals("Vocabulary")) && (!algo.equals("BH1")))) {
                    System.err.print("Vocabulary file must be null for " + algo + ", aborting...\n");
                    System.exit(2);
                } else if ((algo.equals("Vocabulary") || algo.equals("BH1")) && (vocabularyPath.equals("null"))) {
                    System.err.print("Vocabulary file can not be null for " + algo + " , aborting...\n");
                    System.exit(2);
                } else if ((algo.equals("Vocabulary")) && (Files.exists(Paths.get(vocabularyPath)))) {
                    System.err.print("Vocabulary file already exists, aborting...\n");
                    System.exit(2);
                }
                // endregion basic checks arguments

                long start = System.currentTimeMillis(); // Start timer

                String tempPath_1 = outputPath + "_ReqPath_1";
                String tempPath_2 = outputPath + "_ReqPath_2";

                JsonReqPath simValJSON = new JsonReqPath();
                simValJSON.run(inputPath_1, tempPath_1, jsonpath_request_1);
                simValJSON.closeSparkContext();
                if ((!inputPath_2.equals("null")) && (!jsonpath_request_2.equals("null"))) { // Twoway
                    JsonReqPath simValJSON2 = new JsonReqPath();
                    simValJSON2.run(inputPath_2, tempPath_2, jsonpath_request_2);
                    simValJSON2.closeSparkContext();
                }

                if (!algo.equals("JsonReqPath")) {
                    if (algo.equals("Vocabulary")) {
                        if ((inputPath_2.equals("null")) && (jsonpath_request_2.equals("null"))) {
                            Vocabulary vocProgram = new Vocabulary();
                            vocProgram.run(tempPath_1, vocabularyPath, 0);
                            vocProgram.closeSparkContext();
                        } else {
                            JSON.twoway.Vocabulary vocProgram = new JSON.twoway.Vocabulary();
                            vocProgram.run(tempPath_1, tempPath_2, vocabularyPath, 0);
                            vocProgram.closeSparkContext();
                        }
                    } else if (eps < 0) {
                        System.err.print("Distance threshold must be positive for" + algo + ", aborting...\n");
                        System.exit(2);
                    } else {
                        switch (algo) {
                            case "Naive":
                                if ((inputPath_2.equals("null")) && (jsonpath_request_2.equals("null"))) {
                                    Naive naive = new Naive();
                                    naive.run(tempPath_1, outputPath, 0, eps);
                                    naive.closeSparkContext();
                                } else {
                                    JSON.twoway.Naive naive = new JSON.twoway.Naive();
                                    naive.run(tempPath_1, tempPath_2, outputPath, 0, eps);
                                    naive.closeSparkContext();
                                }
                                break;
                            case "BH1":
                                if ((inputPath_2.equals("null")) && (jsonpath_request_2.equals("null"))) {
                                    Vocabulary vocProgram = new Vocabulary();
                                    if ((voc_status == 0) && (!Files.exists(Paths.get(vocabularyPath))))
                                        vocProgram.run(tempPath_1, vocabularyPath, 0);
                                    ArrayList<String> vocabulary = vocProgram.getVocabulary(vocabularyPath);
                                    vocProgram.closeSparkContext();
                                    BH1 bh1 = new BH1();
                                    bh1.run(tempPath_1, outputPath, 0, eps, vocabulary);
                                    bh1.closeSparkContext();
                                } else {
                                    JSON.twoway.Vocabulary vocProgram = new JSON.twoway.Vocabulary();
                                    if ((voc_status == 0) && (!Files.exists(Paths.get(vocabularyPath))))
                                        vocProgram.run(tempPath_1, tempPath_2, vocabularyPath, 0);
                                    ArrayList<String> vocabulary = vocProgram.getVocabulary(vocabularyPath);
                                    vocProgram.closeSparkContext();
                                    JSON.twoway.BH1 bh1 = new JSON.twoway.BH1();
                                    bh1.run(tempPath_1, tempPath_2, outputPath, 0, eps, vocabulary);
                                    bh1.closeSparkContext();
                                }
                                break;
                            case "Splitting":
                                if ((inputPath_2.equals("null")) && (jsonpath_request_2.equals("null"))) {
                                    System.out.println("Splitting Slef Join");
                                    Splitting splitting = new Splitting();
                                    splitting.run(tempPath_1, outputPath, 0, eps);
                                    splitting.closeSparkContext();
                                } else {
                                    System.out.println("Splitting Twoway");

                                    JSON.twoway.Splitting splitting = new JSON.twoway.Splitting();
                                    splitting.run(tempPath_1, tempPath_2, outputPath, 0, eps);
                                    splitting.closeSparkContext();
                                }
                                break;
                            default:
                                System.err.print("Algorithm not found, aborting...\n");
                                System.exit(2);
                                break;
                        }
                    }
                }
                long end = System.currentTimeMillis();
                System.out.println("Time to execute " + algo + " with JsonReqPath: " + (end - start) + " ms");
                // endregion
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}