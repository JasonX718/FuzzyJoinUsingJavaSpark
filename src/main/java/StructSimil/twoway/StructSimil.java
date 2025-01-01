package StructSimil.twoway;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import JSON.pumakey;
import JSON.twoway.BH1;
import StructSimil.GraphCreationMode;
import StructSimil.JsonParseHelper;
import StructSimil.SimilarityHelper;
import StructSimil.SimilarityCompute.CommonStringSimilarity;
import StructSimil.SimilarityCompute.NaiveSimilarity;
import StructSimil.SimilarityCompute.SelectCombinationsSimilarity;
import StructSimil.SimilarityCompute.SimilarityCompute;
import StructSimil.SimilarityCompute.SimilarityComputeMode;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;


/**
 * Structural Similarity join in Spark.
 */
public class StructSimil implements Serializable {
	public static Configuration hadoopConf = new Configuration();
	public static SparkConf sparkconf;
	public static JavaSparkContext javasparkcontext;

	private int modulo;

	private SimilarityCompute similarityCompute;

	public static String hdfsPath = null;

	public void closeSparkContext() {
		StructSimil.javasparkcontext.close();
	}

	public StructSimil(int modulo, SimilarityComputeMode mode) throws ClassNotFoundException {
		sparkconf = new SparkConf().setAppName("Struct JSON fuzzy join");
		javasparkcontext = new JavaSparkContext(sparkconf);
		// hdfsPath = "hdfs://" + CONST.NAMENODE + ":9000";
		hdfsPath = "";

		this.modulo = modulo;

		switch(mode) {
			case NaiveMode:
				this.similarityCompute = new NaiveSimilarity();
				break;
			case CommonStringMode:
				this.similarityCompute = new CommonStringSimilarity();
				break;
			case SelectCombinationsMode:
				this.similarityCompute = new SelectCombinationsSimilarity();
				break;
			default:
				this.similarityCompute = new NaiveSimilarity();
				break;
		}
	}

	public StructSimil(SimilarityComputeMode mode) throws ClassNotFoundException {
		this(10, mode);
	}

	public StructSimil(int modulo) throws ClassNotFoundException {
		this(modulo, SimilarityComputeMode.NaiveMode);
	}

	public StructSimil() throws ClassNotFoundException {
		this(10, SimilarityComputeMode.NaiveMode);
	}

	/**
	 * Create/write a list of pairs of the dataset into a file at a given path
	 * 
	 * @param outputPath : The outpout path to write the result
	 * @param resultData : The list of pairs to be written
	 * @return : true if the data is written successfully, false otherwise
	 */
	public static boolean createResultFile(String outputPath, List<Tuple2<String, String>> resultData) {

        try {
            File file = new File(outputPath);
            if (file.createNewFile())
                System.out.println("Output file created at location: " + file.getCanonicalPath());
            else
                System.out.println("Update the output file at location: " + file.getCanonicalPath());

            // Remove the duplicate
            List<Tuple2<String, String>> tmp = new ArrayList<Tuple2<String, String>>();
            for (Tuple2<String, String> t : resultData) {
                if (!tmp.contains(t))
                    tmp.add(t);
            }

            // Write the result to the file in the format <request|file>
            FileWriter writer = new FileWriter(file);
            for (Tuple2<String, String> t : tmp) {
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
	 * Create JavaPairRDD from JavaRDD. The purpose of the function is to get a list of adjacency matrix base on JSON graph difined into the dataset.
	 * 
	 * @param dataset      : The input dataset
	 * @param key_position : The position of the key
	 * @return : The pairs of the dataset, the adjacency matrix and the associated JSON graph.
	 * @throws NullPointerException
	 */
	public static JavaPairRDD<String, String> createJavaPairRDD(JavaRDD<String> dataset, int key_position, GraphCreationMode mode) throws NullPointerException {
		return dataset.mapToPair(new PairFunction<String,String,String>() {
			ObjectMapper mapper = new ObjectMapper();

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				if (pumakey.isKeyPositionPossible(s, key_position)) {
					String tree = pumakey.getRecordKey(s, key_position);

					String preprocessedTree = preprocessInputToJson(tree);
	
					// Use ObjectMapper to parse the preprocessed JSON string
					JsonNode rootNode = mapper.readTree(preprocessedTree);
	
					// JsonNode rootNode = mapper.readTree(tree);

					List<Tuple2<Integer,Integer>> hashBranchs = JsonParseHelper.getBranchsJsonHash(rootNode, mode);

					int [][] adjacencyMatrix = SimilarityHelper.getAdjacencyMatrixHash(hashBranchs);

					String strAdjacency = SimilarityHelper.matrixToString(adjacencyMatrix);					
					//System.out.println(strAdjacency);

					return new Tuple2<String, String>(strAdjacency, s);
				} else {
					throw new Exception("The key position is not possible");
				}
			}
			private String preprocessInputToJson(String input) {
				// Assuming input is something like "key_value", convert it to {"key": "value"}
				if (input.contains("_")) {
					String[] parts = input.split("_", 2); // Split into key and value
					return String.format("{\"%s\": \"%s\"}", parts[0], parts[1]);
				}
				return String.format("{\"value\": \"%s\"}", input);
			}
		});
	}

	

	/**
	 * Fuzzy join on the adjacency matrix. In order to compute the similarity between two graphs.
	 * 
	 * @param pairRDD_1  : The first dataset containing the adjacency matrix and the associated JSON graph
	 * @param pairRDD_2  : The second dataset containing the adjacency matrix and the associated JSON graph
	 * @param outputPath : The output path to write the result of the fuzzy join
	 * @param eps        : The threshold distance to compute the similarity between two graphs
	 */
	public void fuzzyJoin(JavaPairRDD<String, String> pairRDD_1, JavaPairRDD<String, String> pairRDD_2, String outputPath, int eps, GraphCreationMode mode) {
		
		JavaPairRDD<String, Tuple2<String, String>> couplesRDD = pairRDD_1.join(pairRDD_2);

		JavaPairRDD<String, String> pairCoupleRDD = couplesRDD.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>,String,String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> t) {
				return new Tuple2<String, String>(t._2._1 , t._2._2);
			}
		});

		JavaPairRDD<String,String> pairCoupleDistinctRDD = pairCoupleRDD.distinct();

		/*for(Tuple2<String, String> record : pairCoupleDistinctRDD.collect()){
			String json1 = pumakey.getRecordKey(record._1, 1);
			String json2 = pumakey.getRecordKey(record._2, 1);

			System.out.println(similarityCompute.getClass().getName());

			Tuple2<Double,Integer> resultBestHammingDistance = similarityCompute.bestHammingDistance(json1, json2, mode);

			double distance = resultBestHammingDistance._1;
			int nbNode = resultBestHammingDistance._2;
			double similarity = distance / (double)(nbNode * (nbNode-1));
			double threshold = (double)eps / (double)(nbNode * (nbNode-1));
			
			System.out.println(json1);
			System.out.println(json2);
			System.out.println("Distance : " + distance);
			System.out.println("eps : " + eps);
			System.out.println("Similarity : " + (1-similarity)*100 + "%");
			System.out.println("Threshold : " + (1-threshold)*100+"%");
			System.out.println();
		}*/

		JavaPairRDD<String, String> outputRDD = pairCoupleDistinctRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, String> record) throws Exception {
				List<Tuple2<String, String>> resultList = new ArrayList<>();
				
				if(pumakey.isKeyPositionPossible(record._1, 1) && pumakey.isKeyPositionPossible(record._2, 1)){
					// String json1 = pumakey.getRecordKey(record._1, 1);
					// String json2 = pumakey.getRecordKey(record._2, 1);
					String json1 = preprocessInputToJson(pumakey.getRecordKey(record._1, 1));
        			String json2 = preprocessInputToJson(pumakey.getRecordKey(record._2, 1));

					Tuple2<Double,Integer> resultBestHammingDistance = similarityCompute.bestHammingDistance(json1, json2, mode);

					double distance = resultBestHammingDistance._1;
					int nbNode = resultBestHammingDistance._2;
					double similarity = distance / (double)(nbNode * (nbNode-1));
					double threshold = (double)eps / (double)(nbNode * (nbNode-1));

					System.out.println(distance);
					System.out.println(similarity*100);

					if(similarity <= threshold){
						String result1 = pumakey.getRecordKey(record._1, 1)+"|"+pumakey.getRecordKey(record._1, 2);
						String result2 = pumakey.getRecordKey(record._2, 1)+"|"+pumakey.getRecordKey(record._2, 2);
						System.out.println(pumakey.getRecordKey(record._1, 1));
						System.out.println(pumakey.getRecordKey(record._2, 1));
						Tuple2<String, String> new_similarity = new Tuple2<String, String>(result1, result2);
						if(!resultList.contains(new_similarity))
							resultList.add(new_similarity);
					}
				} else {
					System.out.println("The key position is not possible");
				}
		
				return resultList.iterator();
			}
			private String preprocessInputToJson(String input) {
				if (input.trim().startsWith("{") && input.trim().endsWith("}")) {
					return input;
				}
				return "\"" + input.replace("\"", "\\\"") + "\"";
			}
		});

		JavaPairRDD<String, String> distinctOutputRDD = outputRDD.distinct();

		createResultFile(outputPath+"_Couples", distinctOutputRDD.collect());
	}

	/**
	 * Run the Structural Similarity algorithm. The first step is to create the adjacency matrix of each graph extract from JSON.
	 * Then, the similarity between each graph is computed in the fuzzy fonction. 
	 * Finally, the result is written in a file at the outputPath location.
	 * 
	 * @param inputPath_1  : The first input path
	 * @param inputPath_2  : The second input path
	 * @param outputPath   : The output path
	 * @param key_position : The position of the key
	 * @param eps          : The distance threshold
	 * @throws ClassNotFoundException
	 */
	public void run(String inputPath_1, String inputPath_2, String outputPath, int key_position, int eps, GraphCreationMode mode) {

		//GraphCreationMode mode = GraphCreationMode.SimplifyMode;
		JsonParseHelper.setModulo(modulo);
		SimilarityHelper.setModulo(modulo);

		JavaRDD<String> dataset_1 = javasparkcontext.textFile(hdfsPath + inputPath_1);
		JavaRDD<String> dataset_2 = javasparkcontext.textFile(hdfsPath + inputPath_2);

		JavaPairRDD<String, String> pairRDD_dataset_1 = createJavaPairRDD(dataset_1, key_position, mode);
		JavaPairRDD<String, String> pairRDD_dataset_2 = createJavaPairRDD(dataset_2, key_position, mode);

		JavaRDD<String> merged_pairRDD_dataset_1 = pairRDD_dataset_1.map(pair -> pair._1 + "|" + pair._2);
		JavaRDD<String> merged_pairRDD_dataset_2 = pairRDD_dataset_2.map(pair -> pair._1 + "|" + pair._2);

		int maxLength1 = pumakey.getMaxKeyLength(key_position, merged_pairRDD_dataset_1);
		int maxLength2 = pumakey.getMaxKeyLength(key_position, merged_pairRDD_dataset_2);

		ArrayList<String> vocabulary = new ArrayList<String>() {{add("0");add("1");}};
		JavaPairRDD<String, String> BHpairRDD_dataset_1 = BH1.createJavaPairRDD(merged_pairRDD_dataset_1, vocabulary, key_position, eps, maxLength1, outputPath);
		JavaPairRDD<String, String> BHpairRDD_dataset_2 = BH1.createBHJavaPairRDD(merged_pairRDD_dataset_2, vocabulary, key_position, eps, maxLength2, outputPath);

		createResultFile(outputPath+"_AdjacencyMatrix_1", BHpairRDD_dataset_1.collect());
		createResultFile(outputPath+"_AdjacencyMatrix_2", BHpairRDD_dataset_2.collect());

		fuzzyJoin(BHpairRDD_dataset_1, BHpairRDD_dataset_2, outputPath, eps, mode);

		javasparkcontext.close();
	}
}
