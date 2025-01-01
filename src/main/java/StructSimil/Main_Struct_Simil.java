package StructSimil;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.util.GenericOptionsParser;

import JSON.selfjoins.JsonReqPath;
import StructSimil.SimilarityCompute.SimilarityComputeMode;

public class Main_Struct_Simil {

    /**
     * Main function to runs the structural similarity algorithms with json requests paths.
     * @remark To call the Selfjoins algorithms, path_to_dataset_2 and jsonpath_request_2 must be null.
     * @remark To call the Twoway algorithms, path_to_dataset_2 and jsonpath_request_2 must be specified.
     * @param path_to_dataset_1         : The dataset path 1 <i>(Selfjoins / Twoway)</i>
     * @param path_to_dataset_2         : The dataset path 2 <i>(Selfjoins: null / Twoway: path_to_dataset_2)</i>
     * @param jsonpath_request_1        : The jsonpath request 1 <i>(Selfjoins / Twoway)</i>
     * @param jsonpath_request_2        : The jsonpath request 2 <i>(Selfjoins: null / Twoway: jsonpath_request_2)</i>
     * @param path_to_output            : The output path <i>(Selfjoins / Twoway)</i>
     * @param distance_thresold         : The threshold distance <i>(0 < distance_thresold)</i>
     * @param graph_creation_mode       : The graph creation mode <i>(Simplify / Index / Element)</i>
     * @param similarity_compute_mode   : The similarity compute mode <i>(Naive / CommonString / SelectCombinations)</i>
     */
    public static void main(String[] userargs) {
        try {
            System.out.println("\n\nSTART JSON Struct Similarity ...\n\n");

            // region get arguments
            String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
            if (args.length < 8) {
                System.err.println("Usage: JSON.Struct_Simil <path_to_dataset_1> "
                        + "<path_to_dataset_2> <jsonpath_request_1> <jsonpath_request_2> <path_to_output> <distance_thresold> <graph_creation_mode> <similarity_compute_mode>");
                System.out.println(args.length+" arguments provided");
                for(int i = 0; i < args.length; i++) {
                    System.out.println((i+1)+": "+args[i]);
                }
                System.exit(2);
            } else {
                String inputPath_1 = args[0]; // Dataset path 1
                String inputPath_2 = args[1]; // Dataset path 2
                String jsonpath_request_1 = args[2]; // Dataset path 1
                String jsonpath_request_2 = args[3]; // Dataset path 2
                String outputPath = args[4]+"Struct_Simil"; // Output dataset
                int eps = Integer.parseInt(args[5]); // Distance threshold
                String graph_creation_mode = args[6]; // Graph creation mode
                String similarity_compute_mode = args[7]; // Similarity compute mode
                // endregion get arguments

                System.out.println("Input file 1: " + inputPath_1);
                System.out.println("Input file 2: " + inputPath_2);
                /*System.out.println("Output file: " + outputPath);
                System.out.println("Distance threshold: " + eps);
                System.out.println("Graph creation mode: " + graph_creation_mode);
                System.out.println("Similarity compute mode: " + similarity_compute_mode);*/

                // region basic checks arguments
                // JavaSparkContext javaSparkContext = new JavaSparkContext(new SparkConf().setAppName("Text"));
                if ((!Files.exists(Paths.get(inputPath_1))) || (inputPath_1.equals("null"))) {
                    // System.out.println(javaSparkContext.textFile(inputPath_1).collect());
                    System.err.print("Input file 1 does not exist, aborting...\n");
                    System.exit(2);
                } else if ((!Files.exists(Paths.get(inputPath_2))) && (!inputPath_2.equals("null"))) {
                    // System.out.println(javaSparkContext.textFile(inputPath_2).collect());
                    System.err.print("Input file 2 does not exist, aborting...\n");
                    System.exit(2);
                } else if ((Files.exists(Paths.get(outputPath))) || (outputPath.equals("null"))) {
                    System.err.print("Output file already exists, aborting...\n");
                    System.err.print("Output file: " + Paths.get(outputPath) + "\n");
                    System.exit(2);
                } else if (jsonpath_request_1.equals("null")) {
                    System.err.print("jsonpath_request_1 is not valid, aborting...\n");
                    System.exit(2);
                } else if (((jsonpath_request_2.equals("null") ^ inputPath_2.equals("null")))) {
                    System.err.print("Specify <path_to_dataset_2> and <jsonpath_request_2> or leave them both null, aborting...\n");
                    System.exit(2);
                } else if ((eps <= 0)) {
                    System.err.print("<distance_thresold> should be > 0, aborting...\n");
                    System.exit(2);
                } else if((!graph_creation_mode.equals("Simplify")) && (!graph_creation_mode.equals("Index")) && (!graph_creation_mode.equals("Element"))) {
                    System.err.print("<graph_creation_mode> => "+ graph_creation_mode +" : should be \"Simplify\", \"Index\" or \"Element\", aborting...\n");
                    System.exit(2);
                } else if ((!similarity_compute_mode.equals("Naive")) && (!similarity_compute_mode.equals("CommonString")) && (!similarity_compute_mode.equals("SelectCombinations"))) {
                    System.err.print("<similarity_compute_mode> => "+ similarity_compute_mode +" : should be \"Naive\", \"CommonString\" or \"SelectCombinations\", aborting...\n");
                    System.exit(2);
                }
                // javaSparkContext.close();
                // endregion basic checks arguments

                GraphCreationMode graphCreationMode;
                switch(graph_creation_mode) {
                    case "Simplify":
                        graphCreationMode = GraphCreationMode.SimplifyMode;
                        break;
                    case "Index":
                        graphCreationMode = GraphCreationMode.IndexMode;
                        break;
                    case "Element":
                        graphCreationMode = GraphCreationMode.ElementMode;
                        break;
                    default:
                        graphCreationMode = GraphCreationMode.SimplifyMode;
                        break;
                }

                SimilarityComputeMode similarityComputeMode;
                switch(similarity_compute_mode) {
                    case "Naive":
                        similarityComputeMode = SimilarityComputeMode.NaiveMode;
                        break;
                    case "CommonString":
                        similarityComputeMode = SimilarityComputeMode.CommonStringMode;
                        break;
                    case "SelectCombinations":
                        similarityComputeMode = SimilarityComputeMode.SelectCombinationsMode;
                        break;
                    default:
                        similarityComputeMode = SimilarityComputeMode.NaiveMode;
                        break;
                }

                System.out.println(similarityComputeMode);

                // region run algorithms
                long start = System.currentTimeMillis(); // Start timer

                String tempPath_1 = outputPath + "_ReqPath_1";
                String tempPath_2 = outputPath + "_ReqPath_2";

                System.out.println();
                System.out.println(tempPath_1);
                System.out.println(tempPath_2);
                System.out.println();

                JsonReqPath reqPath = new JsonReqPath();
                reqPath.run(inputPath_1, tempPath_1, jsonpath_request_1);
                if ((!inputPath_2.equals("null")) && (!jsonpath_request_2.equals("null"))) { // Twoway
                    JsonReqPath reqPath2 = new JsonReqPath();
                    reqPath2.run(inputPath_2, tempPath_2, jsonpath_request_2);
                }

                if ((inputPath_2.equals("null")) && (jsonpath_request_2.equals("null"))) {
                    StructSimil.selfjoins.StructSimil struct_simil = new StructSimil.selfjoins.StructSimil(similarityComputeMode);
                    struct_simil.run(tempPath_1, outputPath, 0, eps, graphCreationMode);
                    struct_simil.closeSparkContext();
                } else {
                    StructSimil.twoway.StructSimil struct_simil = new StructSimil.twoway.StructSimil(similarityComputeMode);
                    struct_simil.run(tempPath_1, tempPath_2, outputPath, 0, eps, graphCreationMode);
                    struct_simil.closeSparkContext();
                }

                long end = System.currentTimeMillis();
                System.out.println("Time to execute Struct Similarity with JsonReqPath: " + (end - start) + " ms");
                // endregion
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
