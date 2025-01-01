package StructSimil.SimilarityCompute;

import java.io.Serializable;

import StructSimil.GraphCreationMode;
import scala.Tuple2;

public abstract class SimilarityCompute implements Serializable {
    
    public abstract Tuple2<Double, Integer> bestHammingDistance(String json1, String json2, GraphCreationMode mode);
}
