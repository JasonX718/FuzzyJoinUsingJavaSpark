package StructSimil.SimilarityCompute;

import java.util.Map;

import StructSimil.GraphCreationMode;
import StructSimil.SimilarityHelper;
import scala.Tuple2;

public class SelectCombinationsSimilarity extends SimilarityCompute {

    private SimilarityHelper helper = new SimilarityHelper();

    @Override
    public Tuple2<Double, Integer> bestHammingDistance(String json1, String json2, GraphCreationMode mode) {
        System.out.println("bestHammingDistance SelectCombinationSimilarity two-way");

        helper.getWeightedNode(json1, mode);
        /*for(Map.Entry<Integer,Tuple2<String,Integer>> entry : helper.getWeightedNode(json1, mode).entrySet()) {
            System.out.println("("+entry.getKey()+") => " + entry.getValue()._1 + " : " + entry.getValue()._2);
        }*/
        
        return new Tuple2<Double,Integer>(-1.0, 0);
    }
    
}
