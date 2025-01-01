// package StructSimil.SimilarityCompute;

// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.HashSet;
// import java.util.List;
// import java.util.Set;

// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.google.gson.Gson;
// import com.google.gson.JsonArray;
// import com.google.gson.JsonElement;
// import com.google.gson.JsonObject;
// import com.google.gson.JsonPrimitive;

// import StructSimil.GraphCreationMode;
// import StructSimil.JsonParseHelper;
// import scala.Tuple2;

// public class CommonStringSimilarity extends SimilarityCompute {

//     String pathsToJson(List<String> paths) {
//         JsonObject rootObject = new JsonObject();

//         for (String path : paths) {
//             String[] pathSegments = path.split("\\.");
//             ArrayList<String> pathSegmentsList = new ArrayList<String>(Arrays.asList(pathSegments));
            
//             if(pathSegmentsList.size() > 1) {
//                 pathSegmentsList.remove(0);

//                 if(pathSegmentsList.size() > 2) {
//                     JsonObject currentNode = rootObject;
//                     for (int i = 0; i < pathSegmentsList.size() - 2; i++) {
//                         if (!currentNode.has(pathSegmentsList.get(i))) {
//                             JsonObject newNode = new JsonObject();
//                             currentNode.add(pathSegmentsList.get(i), newNode);
//                             currentNode = newNode;
//                         } else {
//                             currentNode = currentNode.getAsJsonObject(pathSegmentsList.get(i));
//                         }
//                     }

//                     if(currentNode.has(pathSegmentsList.get(pathSegmentsList.size() - 2))) {
//                         JsonElement elementToCheck = currentNode.get(pathSegmentsList.get(pathSegmentsList.size() - 2));
//                         if(elementToCheck.isJsonArray()) {
//                             elementToCheck.getAsJsonArray().add(new JsonPrimitive(pathSegmentsList.get(pathSegmentsList.size() - 1)));
//                         } else {
//                             JsonArray newArray = new JsonArray();
//                             newArray.add(elementToCheck);
//                             newArray.add(new JsonPrimitive(pathSegmentsList.get(pathSegmentsList.size() - 1)));
                            
//                             currentNode.remove(pathSegmentsList.get(pathSegmentsList.size() - 2));
//                             currentNode.add(pathSegmentsList.get(pathSegmentsList.size() - 2), newArray);
//                         }
//                     } else {
//                         currentNode.addProperty(pathSegmentsList.get(pathSegmentsList.size() - 2), pathSegmentsList.get(pathSegmentsList.size() - 1));
//                     }
//                 } else {
//                     JsonObject currentNode = rootObject;
//                     currentNode.addProperty(pathSegmentsList.get(pathSegmentsList.size() - 2), pathSegmentsList.get(pathSegmentsList.size() - 1));
//                 }
//             }    
//         }

//         Gson gson = new Gson();
//         String jsonString = gson.toJson(rootObject);
//         return jsonString;
//     }

//     static String LCSubStr(String s, String t, int n, int m) {
//         // Create DP table
//         int dp[][] = new int[n + 1][m + 1];
//         int maxLength = 0;
//         int endIndex = 0;
      
//         for (int i = 1; i <= n; i++) {
//             for (int j = 1; j <= m; j++) {
//                 if (s.charAt(i - 1) == t.charAt(j - 1)) {
//                     dp[i][j] = dp[i - 1][j - 1] + 1;
//                     if (dp[i][j] > maxLength) {
//                         maxLength = dp[i][j];
//                         endIndex = i - 1;
//                     }
//                 } else {
//                     dp[i][j] = 0;
//                 }
//             }
//         }
      
//         // Extract the longest common substring
//         String longestSubStr = s.substring(endIndex - maxLength + 1, endIndex + 1);
//         return longestSubStr;
//     }
    

//     @Override
//     public Tuple2<Double, Integer> bestHammingDistance(String json1, String json2, GraphCreationMode mode) {
//         System.out.println("bestHammingDistance CommonStringSimilarity two-way");
//         ObjectMapper mapper = new ObjectMapper();

//         double distance = -1.0;
//         int nbNodeNames = 0;
        
//         try {
//             Tuple2<List<String>, Set<String>> tupleResultJson1 = JsonParseHelper.getAllPathToLeafs(mapper.readTree(json1), mode);
//             Tuple2<List<String>, Set<String>> tupleResultJson2 = JsonParseHelper.getAllPathToLeafs(mapper.readTree(json2), mode);
//             List<String> pathsJson1 = tupleResultJson1._1;
//             List<String> pathsJson2 = tupleResultJson2._1;
//             Set<String> nodeNames1 = tupleResultJson1._2;
//             Set<String> nodeNames2 = tupleResultJson2._2;

//             //System.out.println("nodeNames1 : "+nodeNames1);
//             //System.out.println("nodeNames2 : "+nodeNames2);

//             Set<String> allNodeNames = new HashSet<String>(nodeNames1);
//             allNodeNames.addAll(nodeNames2);

//             ArrayList<String> pathToRemoveJson1 = new ArrayList<String>();
//             ArrayList<String> pathToRemoveJson2 = new ArrayList<String>();
//             for(String path1 : pathsJson1) {
//                 for(String path2 : pathsJson2) {
//                     /*String[] words1 = path1.split("\\.");
//                     String[] words2 = path2.split("\\.");
//                     String lastWord1 = words1[words1.length - 1];
//                     String lastWord2 = words2[words2.length - 1];

//                     String commonSubString = LCSubStr(path1, path2, path1.length(), path2.length());
                    
//                     String regex = "^(\\.|ROOT\\.)[\\s\\S]*?(" + Pattern.quote(lastWord1) + "|" + Pattern.quote(lastWord2) + "|\\.)$";
//                     Pattern pattern = Pattern.compile(regex);

//                     System.out.println("path1 (" + path1.length() + ") : "+path1);
//                     System.out.println(lastWord1);
//                     System.out.println("path2 (" + path2.length() + ") : "+path2);
//                     System.out.println(lastWord2);
//                     if(pattern.matcher(commonSubString).matches()) {
//                         System.out.println("LCSubStr ("+commonSubString.length()+") : "+commonSubString);
//                     }
//                     System.out.println();*/
                    
//                     if(!pathToRemoveJson1.contains(path1) && !pathToRemoveJson2.contains(path2) && path1.equals(path2)) {
//                         pathToRemoveJson1.add(path1);
//                         pathToRemoveJson2.add(path2);
//                     }
//                 }
//             }

//             for(String path : pathToRemoveJson1) {
//                 pathsJson1.remove(path);
//             }
//             for(String path : pathToRemoveJson2) {
//                 pathsJson2.remove(path);
//             }

//             distance = 0.0;
//             nbNodeNames = allNodeNames.size();

//             System.out.println("size : " + allNodeNames.size());

//             if(!pathsJson1.isEmpty() || !pathsJson2.isEmpty()) {

//                 String newJson1 = pathsToJson(pathsJson1);
//                 String newJson2 = pathsToJson(pathsJson2);

//                 //System.out.println(pathsJson1+"\n"+pathsJson2+"\n"+newJson1+"\n"+json1+"\n"+newJson2+"\n"+json2);
                
//                 NaiveSimilarity naiveSimilarity = new NaiveSimilarity();
//                 Tuple2<Double,Integer> naiveDistanceResult = naiveSimilarity.bestHammingDistance(newJson1, newJson2, mode);
//                 distance = naiveDistanceResult._1;
//             }
            
//         } catch (Exception e) {
//             distance = -1.0;
//             nbNodeNames = 0;
//             e.printStackTrace();
//         }

//         return new Tuple2<Double, Integer>(distance, nbNodeNames);
//     }
    
// }

package StructSimil.SimilarityCompute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import StructSimil.GraphCreationMode;
import StructSimil.JsonParseHelper;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CommonStringSimilarity extends SimilarityCompute {

    @Override
    public Tuple2<Double, Integer> bestHammingDistance(String json1, String json2, GraphCreationMode mode) {
        System.out.println("bestHammingDistance CommonStringSimilarity two-way");
        ObjectMapper mapper = new ObjectMapper();

        double distance = -1.0;
        int nbNodeNames = 0;

        try {
            // Parse JSON and get the tree structure
            JsonNode rootNode1 = mapper.readTree(json1);
            JsonNode rootNode2 = mapper.readTree(json2);

            // Generate and print the tree structure for both JSONs
            String tree1 = JsonParseHelper.parseJsonToTree(rootNode1, "ROOT", mode);
            String tree2 = JsonParseHelper.parseJsonToTree(rootNode2, "ROOT", mode);

            // Output the tree structures
            System.out.println("Tree Structure of JSON 1:\n" + tree1);
            System.out.println("Tree Structure of JSON 2:\n" + tree2);

            // Continue with the existing logic
            Tuple2<List<String>, Set<String>> tupleResultJson1 = JsonParseHelper.getAllPathToLeafs(rootNode1, mode);
            Tuple2<List<String>, Set<String>> tupleResultJson2 = JsonParseHelper.getAllPathToLeafs(rootNode2, mode);
            List<String> pathsJson1 = tupleResultJson1._1;
            List<String> pathsJson2 = tupleResultJson2._1;
            Set<String> nodeNames1 = tupleResultJson1._2;
            Set<String> nodeNames2 = tupleResultJson2._2;

            Set<String> allNodeNames = new HashSet<>(nodeNames1);
            allNodeNames.addAll(nodeNames2);

            ArrayList<String> pathToRemoveJson1 = new ArrayList<>();
            ArrayList<String> pathToRemoveJson2 = new ArrayList<>();
            for (String path1 : pathsJson1) {
                for (String path2 : pathsJson2) {
                    if (!pathToRemoveJson1.contains(path1) && !pathToRemoveJson2.contains(path2) && path1.equals(path2)) {
                        pathToRemoveJson1.add(path1);
                        pathToRemoveJson2.add(path2);
                    }
                }
            }

            for (String path : pathToRemoveJson1) {
                pathsJson1.remove(path);
            }
            for (String path : pathToRemoveJson2) {
                pathsJson2.remove(path);
            }

            distance = 0.0;
            nbNodeNames = allNodeNames.size();

            System.out.println("Size: " + allNodeNames.size());

            if (!pathsJson1.isEmpty() || !pathsJson2.isEmpty()) {
                String newJson1 = pathsToJson(pathsJson1);
                String newJson2 = pathsToJson(pathsJson2);

                NaiveSimilarity naiveSimilarity = new NaiveSimilarity();
                Tuple2<Double, Integer> naiveDistanceResult = naiveSimilarity.bestHammingDistance(newJson1, newJson2, mode);
                distance = naiveDistanceResult._1;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return new Tuple2<>(distance, nbNodeNames);
    }

    String pathsToJson(List<String> paths) {
        JsonObject rootObject = new JsonObject();

        for (String path : paths) {
            String[] pathSegments = path.split("\\.");
            ArrayList<String> pathSegmentsList = new ArrayList<String>(Arrays.asList(pathSegments));
            
            if(pathSegmentsList.size() > 1) {
                pathSegmentsList.remove(0);

                if(pathSegmentsList.size() > 2) {
                    JsonObject currentNode = rootObject;
                    for (int i = 0; i < pathSegmentsList.size() - 2; i++) {
                        if (!currentNode.has(pathSegmentsList.get(i))) {
                            JsonObject newNode = new JsonObject();
                            currentNode.add(pathSegmentsList.get(i), newNode);
                            currentNode = newNode;
                        } else {
                            currentNode = currentNode.getAsJsonObject(pathSegmentsList.get(i));
                        }
                    }

                    if(currentNode.has(pathSegmentsList.get(pathSegmentsList.size() - 2))) {
                        JsonElement elementToCheck = currentNode.get(pathSegmentsList.get(pathSegmentsList.size() - 2));
                        if(elementToCheck.isJsonArray()) {
                            elementToCheck.getAsJsonArray().add(new JsonPrimitive(pathSegmentsList.get(pathSegmentsList.size() - 1)));
                        } else {
                            JsonArray newArray = new JsonArray();
                            newArray.add(elementToCheck);
                            newArray.add(new JsonPrimitive(pathSegmentsList.get(pathSegmentsList.size() - 1)));
                            
                            currentNode.remove(pathSegmentsList.get(pathSegmentsList.size() - 2));
                            currentNode.add(pathSegmentsList.get(pathSegmentsList.size() - 2), newArray);
                        }
                    } else {
                        currentNode.addProperty(pathSegmentsList.get(pathSegmentsList.size() - 2), pathSegmentsList.get(pathSegmentsList.size() - 1));
                    }
                } else {
                    JsonObject currentNode = rootObject;
                    currentNode.addProperty(pathSegmentsList.get(pathSegmentsList.size() - 2), pathSegmentsList.get(pathSegmentsList.size() - 1));
                }
            }    
        }

        Gson gson = new Gson();
        String jsonString = gson.toJson(rootObject);
        return jsonString;
    }

    static String LCSubStr(String s, String t, int n, int m) {
        // Create DP table
        int dp[][] = new int[n + 1][m + 1];
        int maxLength = 0;
        int endIndex = 0;
      
        for (int i = 1; i <= n; i++) {
            for (int j = 1; j <= m; j++) {
                if (s.charAt(i - 1) == t.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                    if (dp[i][j] > maxLength) {
                        maxLength = dp[i][j];
                        endIndex = i - 1;
                    }
                } else {
                    dp[i][j] = 0;
                }
            }
        }
      
        // Extract the longest common substring
        String longestSubStr = s.substring(endIndex - maxLength + 1, endIndex + 1);
        return longestSubStr;
    }
}
