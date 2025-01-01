package JSON.selfjoins;

import com.nebhale.jsonpath.JsonPath;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

/**
 * Similarity join in Spark.
 * JsonReqPath algorithm
 *
 * @author tttquyen, Rémi Uhartegaray
 */
public class JsonReqPath implements Serializable {
    public static Configuration hadoopConf = new Configuration();
    public static SparkConf sparkconf;
    public static JavaSparkContext javasparkcontext;

    public static String hdfsPath = null;

    /**
     * Close the Spark context.
     */
    public void closeSparkContext() {
        JsonReqPath.javasparkcontext.close();
    }

    /**
     * Create a new instance of JsonReqPath.
     * 
     * @throws ClassNotFoundException
     */
    public JsonReqPath() throws ClassNotFoundException {
        sparkconf = new SparkConf().setAppName("JSON RequestPath");
        javasparkcontext = new JavaSparkContext(sparkconf);
        // hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
        hdfsPath = "";
    }

    /**
     * Create JavaPairRDD from JavaRDD.
     * 
     * @param dataLine    : The string from the dataset
     * @param jsonRequest : The json request
     * @return : List of pairs <request, file> from the string
     */
    public static Iterator<Tuple2<Object, Object>> createresult(String dataLine, String jsonRequest) {

        // Get the request path
        JsonPath requestPath = JsonPath.compile(jsonRequest);

        ArrayList<Tuple2<Object, Object>> out = new ArrayList<Tuple2<Object, Object>>();
        ArrayList<Object> requestList = null;
        try {
            // Get the list of the request
            requestList = requestPath.read(dataLine, ArrayList.class);
            for (Object t : requestList) { // Add to output
                if (t != null)
                    out.add(new Tuple2<Object, Object>(t, dataLine)); // <request, file>
                else
                    requestList.remove(t);
            }
            //System.out.println("Request List Map : " + requestList);
        } catch (Exception e) {
        }
        if (requestList == null) { // Request is not a list
            String requestStr = null;
            try {
                requestStr = requestPath.read(dataLine, String.class);
                if (requestStr != null) {
                    out.add(new Tuple2<Object, Object>(requestStr, dataLine)); // <request, file>
                    //System.out.println("Request String : " + requestStr);
                }
            } catch (Exception e2) {
            }
            if (requestStr == null)  // Request is wrong
                System.err.println("Request not found");
        }
        return out.iterator();
    }

    /**
     * Create JavaPairRDD from JavaRDD.
     * 
     * @param outputPath : The output path
     * @param resultData : The result data
     * @return : True/False : Status of the process
     * @remark : The result is written to the output file in the format <request|file>
     */
    public static boolean createResultFile(String outputPath, List<Tuple2<Object, Object>> resultData) {

        try {
            File file = new File(outputPath);
            if (file.createNewFile())
                System.out.println("Output file created at location: " + file.getCanonicalPath());
            else
                System.out.println("Update the output file at location: " + file.getCanonicalPath());

            // Remove the duplicate
            List<Tuple2<Object, Object>> tmp = new ArrayList<Tuple2<Object, Object>>();
            for (Tuple2<Object, Object> t : resultData) {
                if (!tmp.contains(t))
                    tmp.add(t);
            }

            // Write the result to the file in the format <request|file>
            FileWriter writer = new FileWriter(file);
            for (Tuple2<Object, Object> t : tmp) {
                writer.write(t._1 + "|" + t._2 + "\n");
            }
            writer.close();
            return true;

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Start the JsonReqPath algorithm.
     * 
     * @param inputPath   : The input path
     * @param outputPath  : The output path
     * @param jsonrequest : The json request
     */
    public void run(String inputPath, String outputPath, String jsonrequest) {
        JavaRDD<String> dataset = javasparkcontext.textFile(hdfsPath + inputPath);

        JavaPairRDD<Object, Object> tmp_res = dataset.flatMapToPair(s -> createresult(s, jsonrequest));

        createResultFile(outputPath, tmp_res.collect());

        javasparkcontext.close();
    }

    /**
     * Main function.
     * 
     * @param userarg_0 : path_to_dataset : The dataset path
     * @param userarg_1 : path_to_output : The output path
     * @param userarg_2 : json_request_path : The json request path
     */
    public static void main(String[] userargs) {
        try {
            System.out.println("\n\nSTART JSON SIMILARITY VALUES ...\n\n");

            // region Arguments
            String[] args = new GenericOptionsParser(userargs).getRemainingArgs();
            if (args.length < 3) {
                System.err.println("Usage: JSON.selfjoins.JsonReqPath <path_to_dataset> "
                        + "<path_to_output> <json_request_path>");
                System.exit(2);
            }

            String inputPath = args[0]; // Input dataset
            String outputPath = args[1]; // Output path
            String jsonrequest = args[2]; // Data request

            if ((!Files.exists(Paths.get(inputPath)))) {
                System.err.print("Dataset file does not exists, aborting...\n");
                System.exit(2);
            } else if ((Files.exists(Paths.get(outputPath)))) {
                System.err.print("Output file already exists, aborting...\n");
                System.exit(2);
            } else if (jsonrequest == null) {
                System.err.print("JSON request needs to be specified, aborting...\n");
                System.exit(2);
            }
            // endregion
            long start = System.currentTimeMillis();

            JsonReqPath jsonReqPath = new JsonReqPath();
            jsonReqPath.run(inputPath, outputPath, jsonrequest);

            long end = System.currentTimeMillis();
            System.out.println("Time: " + (end - start) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}