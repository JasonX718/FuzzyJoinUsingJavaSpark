package StructSimil.SimilarityCompute;

import java.util.List;
import java.util.Map;

import StructSimil.GraphCreationMode;
import StructSimil.SimilarityHelper;
import scala.Tuple2;

public class NaiveSimilarity extends SimilarityCompute {
    private SimilarityHelper helper = new SimilarityHelper();

    @Override
    public Tuple2<Double, Integer> bestHammingDistance(String json1, String json2, GraphCreationMode mode) {
        //double bestHammingDistanceMergeNodeName = helper.hammingDistance(json1, json2, helper.getNodeNames(json1, json2, mode), mode);
		double bestHammingDistance = -1.0;
		int distinctValueNodes = 0;

		List<Tuple2<Map<Integer, Integer>, Map<Integer, Integer>>> result = helper.getAllPossibleCombinationsNodeNames(json1, json2, mode);

		for(Tuple2<Map<Integer, Integer>, Map<Integer, Integer>> tuple : result) {
			Map<Integer, Integer> indexNodeNamesJson1ToIndexMatrix = tuple._1;
			Map<Integer, Integer> indexNodeNamesJson2ToIndexMatrix = tuple._2;

			Tuple2<Double, Integer> resultHammingDistance = helper.hammingDistance(json1, json2, indexNodeNamesJson1ToIndexMatrix, indexNodeNamesJson2ToIndexMatrix, mode);
				
			if(bestHammingDistance == -1.0 || resultHammingDistance._1 < bestHammingDistance) {
				bestHammingDistance = resultHammingDistance._1;
				distinctValueNodes = resultHammingDistance._2;
			}
		}
		
		return new Tuple2<Double, Integer>(bestHammingDistance, distinctValueNodes);
    }
    
}
