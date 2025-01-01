package StructSimil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class SimilarityHelper implements Serializable {
    private static int modulo;

	public static void setModulo(int modulo) {
		SimilarityHelper.modulo = modulo;
	}
    
    /**
	 * get the adjacency matrix from a list of branchs (edges) from a graph
	 * 
	 * @param branchs : the list of branchs (edges) to be transformed into adjacency matrix
	 * @return : adjacency matrix representation of the graph
	 */	
	public static int[][] getAdjacencyMatrixHash(List<Tuple2<Integer,Integer>> branchs){
		int[][] adjacencyMatrix = new int[modulo][modulo];

		for(Tuple2<Integer,Integer> branch : branchs){
			adjacencyMatrix[branch._1()][branch._2()] = 1;
		}

		return adjacencyMatrix;
	}

	/**
	 * get the adjacency matrix from a list of branchs (edges) from a graph,
	 * As the difference with the previous method, this one sums the number of edges between two nodes,
	 * For example if there are 3 edges between node 1 and node 2, the value in the matrix will be 3
	 * 
	 * @param branchs : the list of branchs (edges) to be transformed into adjacency matrix
	 * @return : adjacency matrix representation of the graph
	 */	
	/*private int[][] getAdjacencyMatrixSum(List<Tuple2<Integer,Integer>> branchs){
		int[][] adjacencyMatrix = new int[Struct_Simil.modulo][Struct_Simil.modulo];

		for(Tuple2<Integer,Integer> branch : branchs){
			adjacencyMatrix[branch._1()][branch._2()] += 1;
		}

		return adjacencyMatrix;
	}*/

	/**
	 * get the adjacency matrix from a list of branchs (edges) from a graph
	 * 
	 * @param branchs : the list of branchs (edges) to be transformed into adjacency matrix
	 * @return : adjacency matrix representation of the graph
	 */	
	private int[][] getAdjacencyMatrix(List<Tuple2<String,String>> branchs, Map<String, List<Integer>> nodeNames){
		int countNames1 = 0;
		int countNames2 = 0;
		for (Map.Entry<String, List<Integer>> entry : nodeNames.entrySet()) {
			countNames1 += entry.getValue().size();
		}
		for (Map.Entry<String, List<Integer>> entry : nodeNames.entrySet()) {
			countNames2 += entry.getValue().size();
		}

		int[][] adjacencyMatrix = new int[countNames1][countNames2];

		for(Tuple2<String,String> branch : branchs){
			List<Integer> nodeList1 = nodeNames.get(branch._1);
			List<Integer> nodeList2 = nodeNames.get(branch._2);
			int size1 = nodeList1.size();
		  	int size2 = nodeList2.size();

			for (int i = 0; i < size1; i++) {
				int node1 = nodeList1.get(i);
				
				for (int j = 0; j < size2; j++) {
					int node2 = nodeList2.get(j);
					adjacencyMatrix[node1][node2] = 1;
				}
			}
		}

		return adjacencyMatrix;
	}
	
	/**
	 * transform a matrix into a bit array
	 * 
	 * @param matrix : the matrix to be transformed into bit array
	 * @return : array representation of the matrix
	 */	
	/*private int[] matrixToArray(int [][] matrix) {
		int[] array = new int[matrix.length*matrix[0].length];
		int cpt = 0;
		for(int i = 0; i < matrix.length; i++) {
			for(int j = 0; j < Struct_Simil.modulo; j++) {
				array[cpt] = matrix[i][j];
				cpt++;
			}
		}
		return array;
	}*/

	/**
	 * transform a matrix into a bit string 
	 * 
	 * @param matrix : the matrix to be transformed into a bit string
	 * @return : bit string representation of the matrix
	 */	
	public static String matrixToString(int [][] matrix){
		String str = "";
		for(int i = 0; i < matrix.length; i++){
			for(int j = 0; j < matrix[i].length; j++){
				str += matrix[i][j];
			}
		}
		return str;
	}

	public Map<String, List<Integer>> getNodeNames(String json1, String json2, GraphCreationMode mode) {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, List<Integer>> nodeNames = new HashMap<String, List<Integer>>();
		Map<String, List<Integer>> nodeNamesTmp = new HashMap<String, List<Integer>>();

		try {
			JsonNode rootNode1 = mapper.readTree(json1);
			JsonNode rootNode2 = mapper.readTree(json2);

			List<Tuple2<String,String>> branchs1 = new ArrayList<Tuple2<String,String>>();
			List<Tuple2<String,String>> branchs2 = new ArrayList<Tuple2<String,String>>();
			
			switch(mode){
				case SimplifyMode:
					JsonParseHelper.parseJsonSimpleMode(rootNode1, "", branchs1);
					JsonParseHelper.parseJsonSimpleMode(rootNode2, "", branchs2);
					break;
				case IndexMode:
					JsonParseHelper.parseJsonIndexMode(rootNode1, "", branchs1);
					JsonParseHelper.parseJsonIndexMode(rootNode2, "", branchs2);
					break;
				case ElementMode:
					JsonParseHelper.parseJsonElementMode(rootNode1, "", branchs1);
					JsonParseHelper.parseJsonElementMode(rootNode2, "", branchs2);
					break;
				default:
					System.err.println("Error: mode not found -> return empty lists");
					break;
			}

			int cpt = 0;
			nodeNames.put(rootNode1.asText(), new ArrayList<Integer>(Arrays.asList(cpt)));
			for(Tuple2<String,String> branch : branchs1){
				if(nodeNames.containsKey(branch._2)){
					nodeNames.get(branch._2).add(++cpt);
				} else {
					nodeNames.put(branch._2, new ArrayList<Integer>(Arrays.asList(++cpt)));
				}
			}
		
			int cptTmp = 0;
			nodeNamesTmp.put(rootNode2.asText(), new ArrayList<Integer>(Arrays.asList(cptTmp)));
			for(Tuple2<String,String> branch : branchs2){
				if(nodeNamesTmp.containsKey(branch._2)){
					nodeNamesTmp.get(branch._2).add(++cptTmp);
				} else {
					nodeNamesTmp.put(branch._2, new ArrayList<Integer>(Arrays.asList(++cptTmp)));
				}

				if(!nodeNames.containsKey(branch._2)){
					nodeNames.put(branch._2, new ArrayList<Integer>(Arrays.asList(++cpt)));
				} else {
					if(nodeNames.get(branch._2).size() < nodeNamesTmp.get(branch._2).size()){
						nodeNames.get(branch._2).add(++cpt);
					}
				}
			}

			/*int cpt = 0;
			for(Tuple2<String,String> branch : branchs1){
				if(!nodeNames.containsKey(branch._1()))
					nodeNames.put(branch._1(), cpt++);
				if(!nodeNames.containsKey(branch._2()))
					nodeNames.put(branch._2(), cpt++);
			}

			for(Tuple2<String,String> branch : branchs2){
				if(!nodeNames.containsKey(branch._1()))
					nodeNames.put(branch._1(), cpt++);
				if(!nodeNames.containsKey(branch._2()))
					nodeNames.put(branch._2(), cpt++);
			}*/
			
		} catch (JsonProcessingException e) {
			System.err.println("Error: JsonProcessingException : impossible to parse the json : return empty list");
			e.printStackTrace();
		}
		
		return nodeNames;
	}

	private Map<String, List<Integer>> getNodeNames(String json, GraphCreationMode mode) {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, List<Integer>> nodeNames = new HashMap<String, List<Integer>>();

		try {
			JsonNode rootNode = mapper.readTree(json);

			List<Tuple2<String,String>> branchs1 = new ArrayList<Tuple2<String,String>>();
			
			switch(mode){
				case SimplifyMode:
					JsonParseHelper.parseJsonSimpleMode(rootNode, "", branchs1);
					break;
				case IndexMode:
					JsonParseHelper.parseJsonIndexMode(rootNode, "", branchs1);
					break;
				case ElementMode:
					JsonParseHelper.parseJsonElementMode(rootNode, "", branchs1);
					break;
				default:
					System.err.println("Error: mode not found -> return empty lists");
					break;
			}

			int cpt = 0;
			nodeNames.put(rootNode.asText(), new ArrayList<Integer>(Arrays.asList(cpt)));
			for(Tuple2<String,String> branch : branchs1){
				if(nodeNames.containsKey(branch._2)){
					nodeNames.get(branch._2).add(++cpt);
				} else {
					nodeNames.put(branch._2, new ArrayList<Integer>(Arrays.asList(++cpt)));
				}
			}
			
		} catch (JsonProcessingException e) {
			System.err.println("Error: JsonProcessingException : impossible to parse the json : return empty list");
			e.printStackTrace();
		}
		
		return nodeNames;
	}

	public Map<Integer,Tuple2<String,Integer>> getWeightedNode(String json, GraphCreationMode mode) {

		ObjectMapper mapper = new ObjectMapper();
		Map<Integer,Tuple2<String,Integer>> weightedNode = new HashMap<Integer,Tuple2<String,Integer>>();
		
		try {
			JsonNode rootNode = mapper.readTree(json);

			switch (mode) {
				case SimplifyMode:
					JsonParseHelper.parseJsonWeightSimpleModeV2(rootNode, 0, "",  0, false, weightedNode);
					return weightedNode;
				case IndexMode:
					JsonParseHelper.parseJsonWeightIndexMode(rootNode, "", weightedNode);
					return weightedNode;
				case ElementMode:
					JsonParseHelper.parseJsonWeightElementMode(rootNode, "", weightedNode);
				default:
					return weightedNode;
			}
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		return weightedNode;
	}

	public List<Tuple2<Map<Integer, Integer>, Map<Integer, Integer>>> getAllPossibleCombinationsNodeNames(String json1, String json2, GraphCreationMode mode) {
		
		Map<String, List<Integer>> nodeNamesJson1 = getNodeNames(json1, mode);
		Map<String, List<Integer>> nodeNamesJson2 = getNodeNames(json2, mode);

		System.out.println("nodeNamesJson1 : " + nodeNamesJson1);
		System.out.println("nodeNamesJson2 : " + nodeNamesJson2);

		Set<String> allNodeNames = new HashSet<>();
		Set<String> repetitiveNodeNames = new HashSet<>();
		for(Map.Entry<String, List<Integer>> entry : nodeNamesJson1.entrySet()){
			allNodeNames.add(entry.getKey());
			if(entry.getValue().size() > 1){
				repetitiveNodeNames.add(entry.getKey());
			}
		}
		for(Map.Entry<String, List<Integer>> entry : nodeNamesJson2.entrySet()){
			allNodeNames.add(entry.getKey());
			if(entry.getValue().size() > 1){
				repetitiveNodeNames.add(entry.getKey());
			}
		}

		if(repetitiveNodeNames.isEmpty()){
			// Case where there is no repetitive node names
			int indexFinalMatrixCount=0;

			Map<Integer, Integer> indexNodeNamesJson1ToIndexMatrix = new HashMap<Integer, Integer>();
			Map<Integer, Integer> indexNodeNamesJson2ToIndexMatrix = new HashMap<Integer, Integer>();

			for(String nodeName : allNodeNames) {
				if(nodeNamesJson1.containsKey(nodeName)){
					indexNodeNamesJson1ToIndexMatrix.put(nodeNamesJson1.get(nodeName).get(0), indexFinalMatrixCount);
				}
				if(nodeNamesJson2.containsKey(nodeName)){
					indexNodeNamesJson2ToIndexMatrix.put(nodeNamesJson2.get(nodeName).get(0), indexFinalMatrixCount);
				}
				indexFinalMatrixCount++;
			}
		
			List<Tuple2<Map<Integer, Integer>, Map<Integer, Integer>>> result = new ArrayList<Tuple2<Map<Integer, Integer>, Map<Integer, Integer>>>();
			result.add(new Tuple2<Map<Integer,Integer>,Map<Integer,Integer>>(indexNodeNamesJson1ToIndexMatrix, indexNodeNamesJson2ToIndexMatrix));
			return result;
		} else {
			// Case where there is repetitive node names

			Map<String, List<Tuple2<Integer,Integer>>> combinationsNodeNames = new HashMap<String, List<Tuple2<Integer,Integer>>>();

			for(String nodeName : repetitiveNodeNames){
				combinationsNodeNames.put(nodeName, new ArrayList<Tuple2<Integer,Integer>>());
				List<Integer> nodeNameRepetitiveNodeNamesJson1 = nodeNamesJson1.get(nodeName);
				List<Integer> nodeNameRepetitiveNodeNamesJson2 = nodeNamesJson2.get(nodeName);

				for(Integer indexNodeNameJson1 : nodeNameRepetitiveNodeNamesJson1){
					for(Integer indexNodeNameJson2 : nodeNameRepetitiveNodeNamesJson2){
						combinationsNodeNames.get(nodeName).add(new Tuple2<Integer,Integer>(indexNodeNameJson1, indexNodeNameJson2));
					}
				}
			}

			int indexFinalMatrixCount=0;
			Map<Integer, Integer> indexNodeNamesJson1ToIndexMatrix = new HashMap<Integer, Integer>();
			Map<Integer, Integer> indexNodeNamesJson2ToIndexMatrix = new HashMap<Integer, Integer>();

			for(String nodeName : allNodeNames) {
				if(!combinationsNodeNames.containsKey(nodeName)){
					if(nodeNamesJson1.containsKey(nodeName)){
						indexNodeNamesJson1ToIndexMatrix.put(nodeNamesJson1.get(nodeName).get(0), indexFinalMatrixCount);
					}
					if(nodeNamesJson2.containsKey(nodeName)){
						indexNodeNamesJson2ToIndexMatrix.put(nodeNamesJson2.get(nodeName).get(0), indexFinalMatrixCount);
					}
					indexFinalMatrixCount++;
				}
			}
			
			Map<String, List<List<Tuple2<Integer, Integer>>>> allPossibleCombinationOfCombinationMap = new HashMap<String, List<List<Tuple2<Integer, Integer>>>>();
			for(Map.Entry<String, List<Tuple2<Integer, Integer>>> combinationsNodeName : combinationsNodeNames.entrySet()) {
				List<Tuple2<Integer, Integer>> listCombination = combinationsNodeName.getValue();
				List<List<Tuple2<Integer, Integer>>> subLists = new ArrayList<List<Tuple2<Integer, Integer>>>();

				int n = listCombination.size();
				int numCombinations = 1 << n;

				for (int i = 0; i < numCombinations; i++) {
					List<Tuple2<Integer, Integer>> combination = new ArrayList<>();
					for (int j = 0; j < n; j++) {
						if ((i & (1 << j)) != 0) {
							combination.add(listCombination.get(j));
						}
					}
					if(!hasDuplicateFirstIntegers(combination) && !subLists.contains(combination)){
						subLists.add(combination);
					}
				}
				//System.out.println("Num Combinations: " + numCombinations + " Final Num Combination: " + subLists.size());
				allPossibleCombinationOfCombinationMap.put(combinationsNodeName.getKey(), subLists);
			}
			
			//System.out.println(allPossibleCombinationOfCombinationMap);
			List<List<Integer>> listCombinationFinal = new ArrayList<List<Integer>>();
			List<String> listKeys = new ArrayList<String>(allPossibleCombinationOfCombinationMap.keySet());
			listCombinationFinal = recursiveForLoopGetFinalCombination(listKeys, allPossibleCombinationOfCombinationMap, new ArrayList<Integer>(), listCombinationFinal);

			List<Tuple2<Map<Integer, Integer>, Map<Integer, Integer>>> listMapNodeNamesIndexToMatrixIndex = new ArrayList<Tuple2<Map<Integer, Integer>, Map<Integer, Integer>>>();

			for(List<Integer> combination : listCombinationFinal) {
				Integer indexFinalMatrixCountTmp = indexFinalMatrixCount;

				Map<Integer, Integer> indexNodeNamesJson1ToIndexMatrixTmp = new HashMap<Integer, Integer>(indexNodeNamesJson1ToIndexMatrix);
				Map<Integer, Integer> indexNodeNamesJson2ToIndexMatrixTmp = new HashMap<Integer, Integer>(indexNodeNamesJson2ToIndexMatrix);
				for(int i = 0; i < listKeys.size(); i++) {
					String nodeName = listKeys.get(i);

					List<Integer> nodeNameIndex1Tmp = new ArrayList<Integer>(nodeNamesJson1.get(nodeName));
					List<Integer> nodeNameIndex2Tmp = new ArrayList<Integer>(nodeNamesJson2.get(nodeName));

					for(Tuple2<Integer, Integer> tuple : allPossibleCombinationOfCombinationMap.get(nodeName).get(combination.get(i))) {
						indexNodeNamesJson1ToIndexMatrixTmp.put(tuple._1, indexFinalMatrixCountTmp);
						nodeNameIndex1Tmp.remove(tuple._1);
						indexNodeNamesJson2ToIndexMatrixTmp.put(tuple._2, indexFinalMatrixCountTmp);
						nodeNameIndex2Tmp.remove(tuple._2);
						indexFinalMatrixCountTmp++;
					}

					for(Integer index : nodeNameIndex1Tmp) {
						indexNodeNamesJson1ToIndexMatrixTmp.put(index, indexFinalMatrixCountTmp);
						indexFinalMatrixCountTmp++;
					}

					for(Integer index : nodeNameIndex2Tmp) {
						indexNodeNamesJson2ToIndexMatrixTmp.put(index, indexFinalMatrixCountTmp);
						indexFinalMatrixCountTmp++;
					}
					
				}
				listMapNodeNamesIndexToMatrixIndex.add(new Tuple2<Map<Integer, Integer>, Map<Integer, Integer>>(indexNodeNamesJson1ToIndexMatrixTmp, indexNodeNamesJson2ToIndexMatrixTmp));
			}	
			return listMapNodeNamesIndexToMatrixIndex;
		}
	}

	private List<List<Integer>> recursiveForLoopGetFinalCombination(List<String> nodeNamesTmp, Map<String, List<List<Tuple2<Integer, Integer>>>> Map, List<Integer> vals, List<List<Integer>> totalVals) {
		if (nodeNamesTmp.isEmpty()) {
			totalVals.add(new ArrayList<>(vals)); // Ajoute une copie distincte de vals à totalVals
			return totalVals;
		} else {
			String nodeName = nodeNamesTmp.get(0);
			List<String> nodeNamesCopy = new ArrayList<>(nodeNamesTmp);
			nodeNamesCopy.remove(0);
	
			for (int i = 0; i < Map.get(nodeName).size(); i++) {
				List<Integer> valsCopy = new ArrayList<>(vals); // Crée une copie distincte de vals
				valsCopy.add(i);
				totalVals = recursiveForLoopGetFinalCombination(nodeNamesCopy, Map, valsCopy, totalVals);
			}
	
			return totalVals;
		}
	}
	
	private boolean hasDuplicateFirstIntegers(List<Tuple2<Integer, Integer>> subList) {
        List<Integer> firstIntegers = new ArrayList<>();

        for (Tuple2<Integer, Integer> tuple : subList) {
            Integer firstInteger = tuple._1;

            if (firstIntegers.contains(firstInteger)) {
                return true; // Il y a un doublon
            }

            firstIntegers.add(firstInteger);
        }

        return false; // Aucun doublon trouvé
    }

	public double hammingDistance(String json1, String json2, Map<String, List<Integer>> nodeNames, GraphCreationMode mode){
		ObjectMapper mapper = new ObjectMapper();
		double distance = -1;

		try {
			int [][] adjacencyMatrix1 = getAdjacencyMatrix(JsonParseHelper.getBranchsJson(mapper.readTree(json1), mode), nodeNames);
			int [][] adjacencyMatrix2 = getAdjacencyMatrix(JsonParseHelper.getBranchsJson(mapper.readTree(json2), mode), nodeNames);

			double sum = 0;
			for (int i = 0; i < adjacencyMatrix1.length; i++) {
				for (int j = 0; j < adjacencyMatrix1[i].length; j++) {
					sum += Math.abs(adjacencyMatrix1[i][j] - adjacencyMatrix2[i][j]);
				}
			}

			distance = sum / (adjacencyMatrix1.length * (adjacencyMatrix1.length - 1));
		} catch (Exception e) {
			e.printStackTrace();
		}

		return distance;
	}

	public Tuple2<Double, Integer>  hammingDistance(String json1, String json2, Map<Integer, Integer> indexMatrixJson1, Map<Integer, Integer> indexMatrixJson2, GraphCreationMode mode) {
		ObjectMapper mapper = new ObjectMapper();

		double hammingDistance = 0;

		int maxIndexMatrix = 0;
		
		try {
			List<Tuple2<Integer, Integer>> edgesJson1 = JsonParseHelper.getBranchJsonPosition(mapper.readTree(json1), mode);
			List<Tuple2<Integer, Integer>> edgesJson2 = JsonParseHelper.getBranchJsonPosition(mapper.readTree(json2), mode);
			
			for(Integer index : indexMatrixJson1.values()) {
				if(index > maxIndexMatrix) {
					maxIndexMatrix = index;
				}
			}
			for(Integer index : indexMatrixJson2.values()) {
				if(index > maxIndexMatrix) {
					maxIndexMatrix = index;
				}
			}

			int[][] matrixJson1 = new int[maxIndexMatrix+1][maxIndexMatrix+1];
			int[][] matrixJson2 = new int[maxIndexMatrix+1][maxIndexMatrix+1];

			for (int i = 0; i < maxIndexMatrix+1; i++) {
				for (int j = 0; j < maxIndexMatrix+1; j++) {
					matrixJson1[i][j] = 0;
					matrixJson2[i][j] = 0;
				}
			}

			for(Tuple2<Integer, Integer> edge : edgesJson1) {
				matrixJson1[indexMatrixJson1.get(edge._1)][indexMatrixJson1.get(edge._2)] = 1;
			}
			for(Tuple2<Integer, Integer> edge : edgesJson2) {
				matrixJson2[indexMatrixJson2.get(edge._1)][indexMatrixJson2.get(edge._2)] = 1;
			}

			hammingDistance = 0;
			for (int i = 0; i < maxIndexMatrix+1; i++) {
				for (int j = 0; j < maxIndexMatrix+1; j++) {
					hammingDistance += Math.abs(matrixJson1[i][j] - matrixJson2[i][j]);
				}
			}

		} catch (JsonProcessingException e) {
			maxIndexMatrix = 0;
			e.printStackTrace();
		}
		
		return new Tuple2<Double, Integer> (hammingDistance, maxIndexMatrix+1);
	}

    public static int[][] createAdjacencyMatrix(String key, String value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createAdjacencyMatrix'");
    }
}
