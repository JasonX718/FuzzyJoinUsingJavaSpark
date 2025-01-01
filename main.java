import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.util.GenericOptionsParser;

import Database.selfjoins.Naive;
import JSON.pumakey;

/**
 * Similarity join in Spark.
 * BH1 algorithm
 *
 * @author tttquyen, Rémi Uhartegaray
 */

public class Main {

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
            System.out.println("\n\nSTART JSON Main...\n\n");

            // region Arguments

            // region get arguments
            String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
            if (args.length < 9) {
                System.err.println("Usage: JSON.Main <path_to_dataset_1> "
                        + "<path_to_dataset_2> <jsonpath_request_1> <jsonpath_request_2> <path_to_output> <path_to_vocabulary> <algorithm> <distance_thresold> <vocabulary_status>");
                System.exit(2);
            } else {
                System.out.println("Arguments OK");
                String inputPath_1 = args[0]; // Dataset path 1
                String inputPath_2 = args[1]; // Dataset path 2
                String jsonpath_request_1 = args[2]; // Dataset path 1
                String jsonpath_request_2 = args[3]; // Dataset path 2
                String outputPath = args[4]; // Output dataset
                String vocabularyPath = args[5]; // Vocabulary path
                String algorithm = args[6]; // Algorithm
                int eps = Integer.parseInt(args[7]); // Distance threshold
                int voc_status = Integer.parseInt(args[8]); // Vocabulary status
                // Print every arguments
                System.out.println("inputPath_1 = " + inputPath_1);
                System.out.println("inputPath_2 = " + inputPath_2);
                System.out.println("jsonpath_request_1 = " + jsonpath_request_1);
                System.out.println("jsonpath_request_2 = " + jsonpath_request_2);
                System.out.println("outputPath = " + outputPath);
                System.out.println("vocabularyPath = " + vocabularyPath);
                System.out.println("algorithm = " + algorithm);
                System.out.println("eps = " + eps);
                System.out.println("voc_status = " + voc_status);

                // endregion get arguments
                if ((!Files.exists(Paths.get(inputPath_1)))) {
                    System.err.print("Dataset file 1 does not exists, aborting...\n");
                    System.exit(2);
                }
                // region Selfjoin
                if ((inputPath_2.equals("null")) && (jsonpath_request_2.equals("null"))) {
                    // region selfjoin arguments
                    System.out.println("inputPath_2 and jsonpath_request_2 are null, Selfjoin mode");
                    outputPath = outputPath + algorithm;
                    if ((Files.exists(Paths.get(outputPath)))) {
                        System.err.print("Output file already exists, aborting...\n");
                        System.exit(2);
                    } else if (jsonpath_request_1.equals("null")) {
                        System.err.print("jsonpath_request_1 is not valid, aborting...\n");
                        System.exit(2);
                    }
                    if ((algorithm.equals("Naive")) || (algorithm.equals("BH1")) || (algorithm.equals("Splitting"))) {
                        System.out.println("Naive or BH1 or Splitting");
                        if (eps < 0) {
                            System.err.print("Distance threshold must be positive for" + algorithm + ", aborting...\n");
                            System.exit(2);
                        }
                    } 
                    if ((algorithm.equals("BH1")) || (algorithm.equals("Vocabulary"))) {
                        System.out.println("BH1 or Vocabulary");
                        if (Files.exists(Paths.get(vocabularyPath))) {
                            System.err.print("Vocabulary file already exists, aborting...\n");
                            System.exit(2);
                        } else if ((voc_status != 1) && (voc_status != 0)) {
                            System.err.println("voc_status = " + voc_status + " is not valid, aborting...\n");
                            System.exit(2);
                        }
                    }
                    // endregion selfjoin arguments
                    /*if (algorithm == "Naive") {
                        //call naive
                        Naive naive = new Naive();
                        naive.run(inputPath_1, outputPath, eps);
                    }*/
                }
                // endregion
                // region Twoway
                else if ((inputPath_2 != "null") && (jsonpath_request_2 != "null")) {

                }
                // endregion Twoway
                // region Error handling
                else {
                    System.err.print("inputPath_2 and jsonpath_request_2 must be both null or not null, aborting...\n");
                    System.exit(2);
                }
                // endregion Error handling
            }
            // add the name of the algorithm to the output path

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
