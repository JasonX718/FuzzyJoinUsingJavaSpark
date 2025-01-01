package StructSimil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

import JSON.pumakey;
import scala.Tuple2;

public class JsonParseHelper {
	private static int modulo = 10;

	public static void setModulo(int modulo) {
		JsonParseHelper.modulo = modulo;
	}
    
    /**
	 * Get all branchs (edges) of the JSON graph.
	 * this function calls the recursive function according to the selected mode, in order to get all branchs (edges).
	 * 
	 * @param rootNode : the root node of the JSON graph
	 * @param mode : mode to choose the recursive function to call
	 * @return : the list of branchs (edges) of the JSON graph
	 */

	public static String parseJsonToTree(JsonNode rootNode, String parentKey, GraphCreationMode mode) {
		StringBuilder tree = new StringBuilder();
		parseJsonTreeHelper(rootNode, parentKey, mode, tree, 0);
		return tree.toString();
	}
	private static String generateIndent(int indentLevel) {
		StringBuilder indent = new StringBuilder();
		for (int i = 0; i < indentLevel; i++) {
			indent.append("  "); // Add two spaces for each indent level
		}
		return indent.toString();
	}
	
	private static void parseJsonTreeHelper(JsonNode node, String parentKey, GraphCreationMode mode, StringBuilder tree, int indentLevel) {
		String indent = generateIndent(indentLevel); // Use the custom method to generate indent
	
		if (node.isObject()) {
			node.fields().forEachRemaining(entry -> {
				String key = entry.getKey();
				JsonNode childNode = entry.getValue();
	
				// Add the current node to the tree string
				tree.append(indent).append(parentKey).append(" → ").append(key).append("\n");
	
				// Recursively parse child nodes
				parseJsonTreeHelper(childNode, key, mode, tree, indentLevel + 1);
	
				if (childNode.isValueNode()) {
					tree.append(indent).append("  ").append(key).append(" → ").append(childNode.asText()).append("\n");
				}
			});
		} else if (node.isArray()) {
			for (JsonNode arrayElement : node) {
				parseJsonTreeHelper(arrayElement, parentKey, mode, tree, indentLevel + 1);
			}
		} else if (node.isValueNode()) {
			tree.append(indent).append(parentKey).append(" → ").append(node.asText()).append("\n");
		}
	}
	
	


	public static List<Tuple2<Integer,Integer>> getBranchsJsonHash(JsonNode rootNode, GraphCreationMode mode){
		ArrayList<Tuple2<Integer,Integer>> hashBranchs = new ArrayList<Tuple2<Integer,Integer>>();

		switch(mode){
			case SimplifyMode:
				parseJsonSimpleModeHash(rootNode, "", hashBranchs);
				break;
			case IndexMode:
				parseJsonIndexModeHash(rootNode, "", hashBranchs);
				break;
			case ElementMode:
				parseJsonElementModeHash(rootNode, "", hashBranchs);
				break;
			default:
				System.err.println("Error: mode not found -> return empty list");
				break;
		}

		return hashBranchs;
	}

	public static List<Tuple2<Integer, Integer>> getBranchJsonPosition(JsonNode rootNode, GraphCreationMode mode) {
		ArrayList<Tuple2<Integer,Integer>> branchs = new ArrayList<Tuple2<Integer,Integer>>();

		switch(mode){
			case SimplifyMode:
			    parseJsonSimpleModePosition(rootNode, 0, branchs);
				break;
			case IndexMode:
				parseJsonIndexModePosition(rootNode, 0, branchs);
				break;
			case ElementMode:
				parseJsonElementModePosition(rootNode, 0, branchs);
				break;
			default:
				System.err.println("Error: mode not found -> return empty list");
				break;
		}

		return branchs;
	}

	public static List<Tuple2<String,String>> getBranchsJson(JsonNode rootNode, GraphCreationMode mode){
		ArrayList<Tuple2<String,String>> branchs = new ArrayList<Tuple2<String,String>>();

		switch(mode){
			case SimplifyMode:
				parseJsonSimpleMode(rootNode, "", branchs);
				break;
			case IndexMode:
				parseJsonIndexMode(rootNode, "", branchs);
				break;
			case ElementMode:
				parseJsonElementMode(rootNode, "", branchs);
				break;
			default:
				System.err.println("Error: mode not found -> return empty list");
				break;
		}

		return branchs;
	}

	public static Tuple2<List<String>, Set<String>> getAllPathToLeafs(JsonNode rootNode, GraphCreationMode mode){
		List<String> paths = new ArrayList<String>();

		Set<String> nodeNames = new HashSet<String>();

		switch(mode){
			case SimplifyMode:
				findLeafPathsRecSimpleMode(rootNode, "ROOT", paths, nodeNames);
				break;
			case IndexMode:
				findLeafPathsRecIndexMode(rootNode, "ROOT", paths, nodeNames);
				break;
			case ElementMode:
				findLeafPathsRecElementMode(rootNode, "ROOT", paths, nodeNames);
				break;
			default:
				System.err.println("Error: mode not found -> return empty list");
				break;
		}

		return new Tuple2<List<String>,Set<String>>(paths, nodeNames);
	}

    private static void findLeafPathsRecSimpleMode(JsonNode jsonNode, String currentPath, List<String> paths, Set<String> nodeNames) {
        if (jsonNode.isObject()) {
            Iterator<String> fieldNames = jsonNode.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                JsonNode childNode = jsonNode.get(fieldName);
                String childPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;
				nodeNames.add(fieldName);
                findLeafPathsRecSimpleMode(childNode, childPath, paths, nodeNames);
            }
        } else if (jsonNode.isArray()) {
            for (int i = 0; i < jsonNode.size(); i++) {
                JsonNode childNode = jsonNode.get(i);
                findLeafPathsRecSimpleMode(childNode, currentPath, paths, nodeNames);
            }
        } else {
			String fieldName = jsonNode.textValue();
            paths.add(currentPath+"."+fieldName);
			nodeNames.add(fieldName);
        }
    }

	private static void findLeafPathsRecIndexMode(JsonNode jsonNode, String currentPath, List<String> paths, Set<String> nodeNames) {
        if (jsonNode.isObject()) {
            Iterator<String> fieldNames = jsonNode.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                JsonNode childNode = jsonNode.get(fieldName);
                String childPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;
				nodeNames.add(fieldName);
                findLeafPathsRecIndexMode(childNode, childPath, paths, nodeNames);
            }
        } else if (jsonNode.isArray()) {
            for (int i = 0; i < jsonNode.size(); i++) {
                JsonNode childNode = jsonNode.get(i);
                String childPath = currentPath.isEmpty() ? "." + i : currentPath + "." + i;
				nodeNames.add(i+"");
                findLeafPathsRecIndexMode(childNode, childPath, paths, nodeNames);
            }
        } else {
			String fieldName = jsonNode.textValue();
            paths.add(currentPath+"."+fieldName);
			nodeNames.add(fieldName);
        }
    }

	private static void findLeafPathsRecElementMode(JsonNode jsonNode, String currentPath, List<String> paths, Set<String> nodeNames) {
        if (jsonNode.isObject()) {
            Iterator<String> fieldNames = jsonNode.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                JsonNode childNode = jsonNode.get(fieldName);
                String childPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;
				nodeNames.add(fieldName);
                findLeafPathsRecElementMode(childNode, childPath, paths, nodeNames);
            }
        } else if (jsonNode.isArray()) {
            for (int i = 0; i < jsonNode.size(); i++) {
                JsonNode childNode = jsonNode.get(i);
                String childPath = currentPath.isEmpty() ? ".elt" : currentPath + ".elt";
				nodeNames.add("elt");
                findLeafPathsRecElementMode(childNode, childPath, paths, nodeNames);
            }
        } else {
            String fieldName = jsonNode.textValue();
            paths.add(currentPath+"."+fieldName);
			nodeNames.add(fieldName);
        }
    }

	/**
	 * Recusive function to parse the json file and create the list of branchs (edges)
	 * In the case of an "array" node, indexes are not considered when creating branches (edges)
	 * This fonction modifies directly the lists branchs and hashBranchs and returns nothing
	 * 
	 * This fonction is used in the case of SimplifyMode
	 * 
	 * @param node : the current node in the exploration
	 * @param parentKey : name of the key of the parent node
	 * @param branchs : list of branchs (edges) created (as strings)
	 * @param hashBranchs : list of hash of branchs (edges) created (as integers)
	 */
	public static void parseJsonSimpleMode(JsonNode node, String parentKey, List<Tuple2<String,String>> branchs) {
		if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                
				branchs.add(new Tuple2<String,String>(parentKey, key.toString()));
                if(value.isTextual()){
					branchs.add(new Tuple2<String,String>(key.toString(), value.textValue()));
				}
                parseJsonSimpleMode(value, key, branchs);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);
                
				if(arrayElement.isTextual()) {
					branchs.add(new Tuple2<String,String>(parentKey, arrayElement.textValue()));
				}
				
				parseJsonSimpleMode(arrayElement, parentKey, branchs);
            }
        }
	}

	public static void parseJsonSimpleModeHash(JsonNode node, String parentKey, List<Tuple2<Integer,Integer>> hashBranchs) {
		if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                
				hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(parentKey.toCharArray(), modulo), pumakey.getHashCode(key.toString().toCharArray(), modulo)));
                if(value.isTextual()){
					hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(key.toString().toCharArray(), modulo), pumakey.getHashCode(value.textValue().toCharArray(), modulo)));
				}
                parseJsonSimpleModeHash(value, key, hashBranchs);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);
                
				if(arrayElement.isTextual()) {
					hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(parentKey.toCharArray(), modulo), pumakey.getHashCode(arrayElement.textValue().toCharArray(), modulo)));
				}
				
				parseJsonSimpleModeHash(arrayElement, parentKey, hashBranchs);
            }
        }
	}

    public static int parseJsonSimpleModePosition(JsonNode node, int parentCount, List<Tuple2<Integer,Integer>> branchs) {
		int cptTmp = parentCount;
		
        if (node.isObject()) {
            Iterator<Map.Entry<String,JsonNode>> nodes = node.fields();

			while(nodes.hasNext()){
				Map.Entry<String,JsonNode> entry = nodes.next();
				JsonNode value = entry.getValue();
				
				branchs.add(new Tuple2<Integer,Integer>(parentCount, ++cptTmp));
				if(value.isTextual()){
					branchs.add(new Tuple2<Integer,Integer>(cptTmp, ++cptTmp));
				} else {
					cptTmp = parseJsonSimpleModePosition(value, cptTmp, branchs);
				}
			}
		} else if (node.isArray()) {
			for (int i = 0; i < node.size(); i++) {
				JsonNode arrayElement = node.get(i);
				
				if(arrayElement.isTextual()) {
					branchs.add(new Tuple2<Integer,Integer>(parentCount, ++cptTmp));
				} else {
					cptTmp = parseJsonSimpleModePosition(arrayElement, cptTmp, branchs);
				}
			}
		}
		return cptTmp;
    }

	/**
	 * Recusive function to parse the json file and create the list of branchs (edges)
	 * In the case of an "array" node, indexes are considered when creating branches (edges)
	 * This fonction modifies directly the lists branchs and hashBranchs and returns nothing
	 * 
	 * This fonction is used in the case of IndexMode
	 * 
	 * @param node : the current node in the exploration
	 * @param parentKey : name of the key of the parent node
	 * @param branchs : list of branchs (edges) created (as strings)
	 * @param hashBranchs : list of hash of branchs (edges) created (as integers)
	 */
	public static void parseJsonIndexMode(JsonNode node, String parentKey, List<Tuple2<String,String>> branchs) {
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                
				branchs.add(new Tuple2<String,String>(parentKey, key.toString()));
				//System.out.println("("+parentKey + "," + key + ")");
				if(value.isTextual()) {
					branchs.add(new Tuple2<String,String>(key.toString(), value.textValue()));
					//System.out.println("("+key + "," + value.textValue() + ")");
				}
                
                parseJsonIndexMode(value, key, branchs);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);
                
				branchs.add(new Tuple2<String,String>(parentKey, Integer.toString(i)));
				//System.out.println("("+parentKey + "," + i + ")");
				if(arrayElement.isTextual()) {
					branchs.add(new Tuple2<String,String>(Integer.toString(i), arrayElement.textValue()));
					//System.out.println("("+i + "," + arrayElement.textValue() + ")");
				}
                
                parseJsonIndexMode(arrayElement, Integer.toString(i), branchs);
            }
        }
    }

	public static void parseJsonIndexModeHash(JsonNode node, String parentKey, List<Tuple2<Integer,Integer>> hashBranchs) {
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                
				hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(parentKey.toCharArray(), modulo), pumakey.getHashCode(key.toString().toCharArray(), modulo)));
				//System.out.println("("+parentKey + "," + key + ")");
				if(value.isTextual()) {
					hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(key.toString().toCharArray(), modulo), pumakey.getHashCode(value.textValue().toCharArray(), modulo)));
					//System.out.println("("+key + "," + value.textValue() + ")");
				}
                
                parseJsonIndexModeHash(value, key, hashBranchs);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);
                
				hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(parentKey.toCharArray(), modulo), pumakey.getHashCode(Integer.toString(i).toCharArray(), modulo)));
				//System.out.println("("+parentKey + "," + i + ")");
				if(arrayElement.isTextual()) {
					hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(Integer.toString(i).toCharArray(), modulo), pumakey.getHashCode(arrayElement.textValue().toCharArray(), modulo)));
					//System.out.println("("+i + "," + arrayElement.textValue() + ")");
				}
                
                parseJsonIndexModeHash(arrayElement, Integer.toString(i), hashBranchs);
            }
        }
    }

    public static int parseJsonIndexModePosition(JsonNode node, int parentCount, List<Tuple2<Integer,Integer>> branchs) {
        int cptTmp = parentCount;
		
        if (node.isObject()) {
            Iterator<Map.Entry<String,JsonNode>> nodes = node.fields();

			while(nodes.hasNext()){
				Map.Entry<String,JsonNode> entry = nodes.next();
				JsonNode value = entry.getValue();
				
				branchs.add(new Tuple2<Integer,Integer>(parentCount, ++cptTmp));
				if(value.isTextual()){
					branchs.add(new Tuple2<Integer,Integer>(cptTmp, ++cptTmp));
				} else {
					cptTmp = parseJsonSimpleModePosition(value, cptTmp, branchs);
				}
			}
		} else if (node.isArray()) {
			for (int i = 0; i < node.size(); i++) {
				JsonNode arrayElement = node.get(i);
				
				branchs.add(new Tuple2<Integer,Integer>(parentCount, ++cptTmp));
				if(arrayElement.isTextual()) {
					branchs.add(new Tuple2<Integer,Integer>(cptTmp, ++cptTmp));
				} else {
					cptTmp = parseJsonSimpleModePosition(arrayElement, cptTmp, branchs);
				}
			}
		}
		return cptTmp;
    }

	/**
	 * Recusive function to parse the json file and create the list of branchs (edges)
	 * In the case of an "array" node, indexes are considered, be replace by "elt" when creating branches (edges)
	 * This fonction modifies directly the lists branchs and hashBranchs and returns nothing
	 * 
	 * This fonction is used in the case of IndexMode
	 * 
	 * @param node : the current node in the exploration
	 * @param parentKey : name of the key of the parent node
	 * @param branchs : list of branchs (edges) created (as strings)
	 * @param hashBranchs : list of hash of branchs (edges) created (as integers)
	 */
	public static void parseJsonElementMode(JsonNode node, String parentKey, List<Tuple2<String,String>> branchs) {
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                
				branchs.add(new Tuple2<String,String>(parentKey, key.toString()));
				//System.out.println("("+parentKey + "," + key + ")");
				if(value.isTextual()) {
					branchs.add(new Tuple2<String,String>(key.toString(), value.textValue()));
					//System.out.println("("+key + "," + value.textValue() + ")");
				}
                
                parseJsonElementMode(value, key, branchs);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);

				String elementStr = "elt";
                
				branchs.add(new Tuple2<String,String>(parentKey, elementStr));
				//System.out.println("("+parentKey + ","+elementStr+")");
				if(arrayElement.isTextual()) {
					branchs.add(new Tuple2<String,String>(elementStr, parentKey));
					//System.out.println("("+elementStr+"," + arrayElement.textValue() + ")");
				}
                
                parseJsonElementMode(arrayElement, Integer.toString(i), branchs);
            }
        }
    }

	public static void parseJsonElementModeHash(JsonNode node, String parentKey, List<Tuple2<Integer,Integer>> hashBranchs) {
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                
				hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(parentKey.toCharArray(), modulo), pumakey.getHashCode(key.toString().toCharArray(), modulo)));
				//System.out.println("("+parentKey + "," + key + ")");
				if(value.isTextual()) {
					hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(key.toString().toCharArray(), modulo), pumakey.getHashCode(value.textValue().toCharArray(), modulo)));
					//System.out.println("("+key + "," + value.textValue() + ")");
				}
                
                parseJsonElementModeHash(value, key, hashBranchs);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);

				String elementStr = "elt";
                
				hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(parentKey.toCharArray(), modulo), pumakey.getHashCode(elementStr.toCharArray(), modulo)));
				//System.out.println("("+parentKey + ","+elementStr+")");
				if(arrayElement.isTextual()) {
					hashBranchs.add(new Tuple2<Integer,Integer>(pumakey.getHashCode(elementStr.toCharArray(), modulo), pumakey.getHashCode(parentKey.toCharArray(), modulo)));
					//System.out.println("("+elementStr+"," + arrayElement.textValue() + ")");
				}
                
                parseJsonElementModeHash(arrayElement, Integer.toString(i), hashBranchs);
            }
        }
    }

    public static int parseJsonElementModePosition(JsonNode node, int parentCount, List<Tuple2<Integer,Integer>> branchs) {
        int cptTmp = parentCount;
		
        if (node.isObject()) {
            Iterator<Map.Entry<String,JsonNode>> nodes = node.fields();

			while(nodes.hasNext()){
				Map.Entry<String,JsonNode> entry = nodes.next();
				JsonNode value = entry.getValue();
				
				branchs.add(new Tuple2<Integer,Integer>(parentCount, ++cptTmp));
				if(value.isTextual()){
					branchs.add(new Tuple2<Integer,Integer>(cptTmp, ++cptTmp));
				} else {
					cptTmp = parseJsonSimpleModePosition(value, cptTmp, branchs);
				}
			}
		} else if (node.isArray()) {
			int elementCount = ++cptTmp;
			for (int i = 0; i < node.size(); i++) {
				JsonNode arrayElement = node.get(i);
				
				branchs.add(new Tuple2<Integer,Integer>(parentCount, elementCount));
				if(arrayElement.isTextual()) {
					branchs.add(new Tuple2<Integer,Integer>(elementCount, ++cptTmp));
				} else {
					cptTmp = parseJsonSimpleModePosition(arrayElement, elementCount, branchs);
				}
			}
		}
		return cptTmp;
    }

	public static Tuple2<Integer, Integer> parseJsonWeightSimpleModeV2(JsonNode node, int parentCount, String parentKey, int nodeParent, boolean isCall, Map<Integer,Tuple2<String,Integer>> nodeWeight) {
        int cptTmp = parentCount;
		int countChildren = 0;
		
        if (node.isObject()) {
            Iterator<Map.Entry<String,JsonNode>> nodes = node.fields();

			while(nodes.hasNext()){
				Map.Entry<String,JsonNode> entry = nodes.next();
				String key = entry.getKey();
				JsonNode value = entry.getValue();
				
				System.out.println(parentKey+" "+key+" "+value+" ("+value.getNodeType()+") : cptTmp "+cptTmp+" countChildren "+countChildren);
				if(value.isValueNode()) {
					nodeWeight.put(++cptTmp, new Tuple2<String,Integer>(key, nodeParent+1));
					String fieldName = value.textValue();
            		nodeWeight.put(++cptTmp, new Tuple2<String,Integer>(fieldName, 1));
				} else {
					Tuple2<Integer,Integer> result = parseJsonWeightSimpleModeV2(value, cptTmp, key, 1, false, nodeWeight);
					cptTmp += result._1;
				}
			}
		} else if (node.isArray()) {

			boolean show = false;
			if(cptTmp == 14) 
				show = true;

			for (int i = 0; i < node.size(); i++) {
				JsonNode arrayElement = node.get(i);
				
				if(arrayElement.isValueNode()) {
					String fieldName = arrayElement.textValue();
					nodeWeight.put(++cptTmp, new Tuple2<String,Integer>(fieldName, 1));
					if(show)
						System.out.println(cptTmp);
					countChildren++;
				} else {
					System.out.println(parentKey+" ("+i+") cmpTmp avant : " + cptTmp + ", countChildren : " + countChildren);
					Tuple2<Integer,Integer> result = parseJsonWeightSimpleModeV2(arrayElement, cptTmp, parentKey, 1, true, nodeWeight);
					cptTmp += result._1;
					countChildren += result._2;
					System.out.println(parentKey+" ("+i+") cmpTmp apres : " + cptTmp + ", countChildren : " + countChildren);
				}
			}
			if(!isCall) {
				nodeWeight.put(parentCount, new Tuple2<String,Integer>(parentKey, countChildren));
			}
		}
		return new Tuple2<Integer,Integer>(cptTmp, countChildren);
    }

	public static int parseJsonWeightSimpleMode(JsonNode node, String parentKey, int nodeParent, boolean isCall, Map<Integer,Tuple2<String,Integer>> nodeWeigh) {
		/*if (node.isObject()) {
            nodeWeight.add(new Tuple2<String,Integer>(parentKey, node.size()+nodeParent));
			node.fields().forEachRemaining(entry -> {
				String key = entry.getKey();
				JsonNode value = entry.getValue();

				if(value.isValueNode()) {
					nodeWeight.add(new Tuple2<String,Integer>(key, nodeParent+1));
					String fieldName = value.textValue();
            		nodeWeight.add(new Tuple2<String,Integer>(fieldName, 1));
				} else {
					parseJsonWeightSimpleMode(value, key, 1, false, nodeWeight);
				}
			});
			return node.size();
        } else if (node.isArray()) {
			int countChildren = 0;
			for (int i = 0; i < node.size(); i++) {
				JsonNode arrayElement = node.get(i);
				
				if(arrayElement.isValueNode()) {
					String fieldName = arrayElement.textValue();
					nodeWeight.add(new Tuple2<String,Integer>(fieldName, 1));
					countChildren++;
				} else {
					countChildren += parseJsonWeightSimpleMode(arrayElement, parentKey, 1, true, nodeWeight);
				}
				
			}
			if(!isCall) {
				nodeWeight.add(new Tuple2<String,Integer>(parentKey, countChildren));
			}
			return countChildren;
        }*/
		return 0;
	}

	public static void parseJsonWeightIndexMode(JsonNode node, String parentKey, Map<Integer,Tuple2<String,Integer>> nodeWeigh) {
		/*if (node.isObject()) {
            nodeWeight.add(new Tuple2<String,Integer>(parentKey, node.size()+1));
			node.fields().forEachRemaining(entry -> {
				String key = entry.getKey();
				JsonNode value = entry.getValue();
				
				parseJsonWeightIndexMode(value, key, nodeWeight);
			});
        } else if (node.isArray()) {
            nodeWeight.add(new Tuple2<String,Integer>(parentKey, node.size()+1));
			for (int i = 0; i < node.size(); i++) {
				JsonNode arrayElement = node.get(i);
				
				parseJsonWeightIndexMode(arrayElement, Integer.toString(i), nodeWeight);
			}
        } else {
			String fieldName = node.textValue();
            nodeWeight.add(new Tuple2<String,Integer>(fieldName, 1));
        }*/
	}

	public static void parseJsonWeightElementMode(JsonNode node, String parentKey, Map<Integer,Tuple2<String,Integer>> nodeWeigh) {

	}
}
