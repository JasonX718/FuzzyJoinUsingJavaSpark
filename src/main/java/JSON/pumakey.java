package JSON;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Random;

import org.apache.spark.api.java.JavaRDD;

import filters.CONST;
import filters.CONST_FILTERS;
import scala.Tuple2;

public class pumakey {
    public static ArrayList<String> vocabulary = new ArrayList<String>();

    public static int getRandomCode(int j) {

        Random r = new Random();
        int code = Math.abs(r.nextInt());
        return code % j;
    }

    public static int getHashCode(char[] str, int j) {

        int code = 0;

        for (int i = 0; i < str.length; i++) {
            code += str[i] * 31 ^ (i);
        }

        return code % j;
    }

    /* key: entryNum64467563485 */
    /*
     * public static char[] getKey(String str, int keyCol) {
     * try {
     * //return str.split(",")[keyCol].substring(8, 19).toCharArray();
     * String tmp = str.split(",")[keyCol].substring(8);
     *
     * char[] ch= tmp.toCharArray();
     * //System.out.println("char[] " + String.valueOf(ch));
     * if(ch.length!=CONSTANTS.LENGTH) return ch;
     * else return null;
     * }
     * catch (IndexOutOfBoundsException e) {
     * return null;
     * }
     * }
     */
    public static String get2Keys(String str, int col0, int col1) {

        String tmpstr = pumakey.getStrKey(str, col0);
        tmpstr = tmpstr.concat(",");
        tmpstr = tmpstr.concat(pumakey.getStrKey(str, col1));
        return tmpstr;
    }

    public static String getStrKey(String str, int keyCol) {
        try {
            // return str.split(",")[keyCol].substring(8, 19).toCharArray();
            String[] splits = str.split(",");
            // if (splits.length>=CONST.NUM_COL) {
            if (splits.length >= keyCol) {
                // String key = str.split(",")[keyCol];
                String key = splits[keyCol];
                return key.substring(key.length() - CONST.KEY_MINI_LENGTH);
            } else
                return "";
        } catch (IndexOutOfBoundsException e) {
            return "";
        }
    }

    public static String getStrKey1(String str, int keyCol) {
        try {
            // return str.split(",")[keyCol].substring(8, 19).toCharArray();
            String[] splits = str.split(",");
            // if (splits.length>=CONST.NUM_COL) {
            if (splits.length >= keyCol) {
                // String key = str.split(",")[keyCol];
                String key = splits[keyCol];
                return key.substring(key.length() - CONST.KEY_MINI_LENGTH - 1);
            } else
                return "";
        } catch (IndexOutOfBoundsException e) {
            return "";
        }
    }

    /**
     * Check if the key is in the list
     *
     * @param str    : Dataset to check
     * @param keyCol : Column of the key
     * @return
     */
    public static boolean isKeyPositionPossible(String str, int keyCol) {

        String[] splits = str.split("\\|");
        if (splits.length >= keyCol) {
            return true;
        } else
            return false;
    }

    /**
     * Get the key of the record
     *
     * @param dataset : Dataset
     * @param keyCol  : Column of the key
     * @return
     */
    public static String getRecordKey(String dataset, int keyCol) {
        try {
            String[] splits = dataset.split("\\|");
            return splits[keyCol];
        } catch (IndexOutOfBoundsException e) {
            return "";
        }
    }

    public static String getCountKey(String str, int keyCol) {
        try {
            // return str.split(",")[keyCol].substring(8, 19).toCharArray();
            String key = str.split(",")[keyCol];
            return "k".concat(key.substring(key.length() - CONST.KEY_MINI_LENGTH - 1, key.length() - 1));
        } catch (IndexOutOfBoundsException e) {
            return "";
        }
    }

    public static String replaceKey(String str, int keyCol, String c) {
        String newstr = new String(str);
        String key = newstr.split(",")[keyCol];
        key = key.substring(key.length() - CONST.KEY_MINI_LENGTH);
        String replace = key.substring(0, key.length() - 1).concat(c);
        newstr = newstr.replaceAll(key, replace);
        return newstr;
    }

    public static String replaceKey5(String str, int keyCol, String c) {
        String newstr = new String(str);
        String key = newstr.split(",")[keyCol];
        key = key.substring(key.length() - CONST.KEY_MINI_LENGTH);
        String replace = key.substring(0, key.length() - 5).concat(c);
        newstr = newstr.replaceAll(key, replace);
        return newstr;
    }

    public static String replaceKey1(String str, int keyCol, String c) {
        String newstr = new String(str);
        String key = newstr.split(",")[keyCol];
        key = key.substring(key.length() - CONST.KEY_MINI_LENGTH);
        // String replace = key.substring(0,key.length()-1).concat(c);
        String replace = key.concat(c);
        newstr = newstr.replaceAll(key, replace);
        return newstr;
    }

    public static int getHashKey(String str, int keyCol) {
        try {
            // return str.split(",")[keyCol].substring(8, 19).toCharArray();
            String tmpstr = str.split(",")[keyCol];
            tmpstr = tmpstr.substring(tmpstr.length() - CONST.KEY_MINI_LENGTH);
            // if(tmpstr.length()!=CONST.LENGTH || str.contains("R") || str.contains("S"))
            // return -1;
            if (tmpstr.length() != CONST.KEY_MINI_LENGTH)
                return -1;
            return Math.abs(CONST_FILTERS.hashFunction.hash(tmpstr.getBytes())) % CONST_FILTERS.vectorsize;
        } catch (IndexOutOfBoundsException e) {
            return -1;
        }
    }

    // TODO : Remove this function for Vocabulary.java function
    public static void getVocabulary(String vocabularyPath) throws FileNotFoundException {

        File file = new File(vocabularyPath);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                if (!vocabulary.contains(line))
                    vocabulary.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // TODO : Remove this function for Vocabulary.java function
    public static boolean createVocabulary(String vocabularyPath, String str) {

        File file = new File(vocabularyPath);

        try {
            boolean result = file.createNewFile();

            if (result)
                System.out.println("file created " + file.getCanonicalPath());

            char[] tmpstr = str.toCharArray();
            for (char c : tmpstr) {
                if (!vocabulary.contains(String.valueOf(c)))
                    vocabulary.add(String.valueOf(c));
            }
            Files.write(file.toPath(), vocabulary);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /*
     * public static int getDistanceBetween(String str1, String str2)
     * {
     * int dist = 0;
     * //for(int i=0; i < elems.length; i++)
     * for(int i=8; i < 19; i++)
     * {
     * if(str1.charAt(i) != str2.charAt(i))
     * dist++;
     * }
     * return dist;
     * }
     */
    /*
     * public static int getDistance(String str1, String str2) {
     * int dist = 0;
     * for (int i = 0; i < str1.length(); i++) {
     * if (str1.charAt(i) != str2.charAt(i))
     * dist++;
     * }
     * return dist;
     * }
     */
    /*
     * public static boolean isSimilair2(String str1, String str2, int d) {
     * int dist = 0, str1Length = str1.length() - 1, str2Length = str2.length() - 1;
     * if ((str1.length() == 0) || (str2.length() == 0))
     * return false;
     * else {
     * System.out.println("str1 : " + str1 + " str2 : " + str2 + " str1Length : " +
     * str1Length + " str2Length : " + str2Length + " dist : " + dist);
     * while (dist <= d && str1Length >= 0) {
     * try {
     * if (str1.charAt(str1Length) != str2.charAt(str1Length)){
     * dist++;
     * System.out.println("!= str1 : " + str1 + " str2 : " + str2 + " str1Length : "
     * + str1Length + " str2Length : " + str2Length + " dist : " + dist);
     * }
     * } catch (IndexOutOfBoundsException e) {
     * System.out.println("Error on similiarity check, aborting...");
     * return false;
     * }
     * str1Length--;
     * }
     * if (dist <= d) {
     * System.out.println("OUI str1 : " + str1 + " str2 : " + str2 + " dist : " +
     * dist);
     * return true;
     * } else {
     * System.out.println("NON str1 : " + str1 + " str2 : " + str2 + " dist : " +
     * dist);
     * return false;
     * }
     * }
     * }
     */
    public static boolean isSimilair2(String str1, String str2, int d) {
        int dist = 0, str1Length = str1.length() - 1, str2Length = str2.length() - 1;
        if ((str1.length() == 0) || (str2.length() == 0))
            return false;
        else {
            System.out.println("str1 : " + str1 + " str2 : " + str2 + " str1Length : " +
                    str1Length + " str2Length : " + str2Length + " dist : " + dist);
            while (dist <= d && str1Length >= 0) {
                try {
                    if (str1.charAt(str1Length) != str2.charAt(str1Length)) {
                        dist++;
                        System.out.println("!= str1 : " + str1 + " str2 : " + str2 + " str1Length : "
                                + str1Length + " str2Length : " + str2Length + " dist : " + dist);
                    }
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Error on similiarity check, aborting...");
                    return false;
                }
                str1Length--;
            }
            if (dist <= d) {
                System.out.println("OUI str1 : " + str1 + " str2 : " + str2 + " dist : " +
                        dist);
                return true;
            } else {
                System.out.println("NON str1 : " + str1 + " str2 : " + str2 + " dist : " +
                        dist);
                return false;
            }
        }
    }

    public static String getAdditionalChar(ArrayList<String> vocabulary) {

        String additionalChar = null;
        int cpt = 0x0021; // start at !, the first printable char

        if (vocabulary.size() == 0xFFFF) {
            System.err.println("Error GetAdditionalChar: vocabulary is full");
            return null;
        } else {
            while (additionalChar == null && cpt < 0xFFFF) {
                if (!vocabulary.contains(String.valueOf((char) cpt))) {
                    additionalChar = String.valueOf((char) cpt);
                    System.out.println("additionalChar " + additionalChar);
                    return additionalChar;
                }
                cpt++;
            }
            System.err.println("Error GetAdditionalChar: no additional char found");
            return null;
        }
    }

    public static int getDistanceBetween(String strMin, String strMax) {
        int dist = 0;
        int j = 0;
        for (int i = 0; i < strMin.length() && j < strMax.length(); i++) {
            if (strMin.charAt(i) != strMax.charAt(j)) {
                dist++;
                i--;
            }
            // if we are at the end of the shortest string, we add the remaining
            if (i == strMin.length() - 1) {
                dist += strMax.length() - j - 1;
                break;
            }
            j++;
        }
        return dist;
    }

    public static boolean isSimilair(String str1, String str2, int d) {
        int dist = 0, str1Length = str1.length() - 1, str2Length = str2.length() - 1;
        if ((str1.length() == 0) || (str2.length() == 0))
            return false;
        else {
            if (str1Length > str2Length) {
                dist = getDistanceBetween(str2, str1);
            } else if (str1Length < str2Length) {
                dist = getDistanceBetween(str1, str2);
            } else {
                for (int i = 0; i < str1.length(); i++) {
                    if (str1.charAt(i) != str2.charAt(i))
                        dist++;
                }
            }
            if (dist <= d)
                return true;
            else
                return false;

        }
    }

    public static int getMaxKeyLength(int key_position, JavaRDD<String> dataset_1, JavaRDD<String> dataset_2) {
        // Get the maximum length of the keys at the key position
        int maxKeyLength = 0;
        for (String t : dataset_1.collect()) {
            if (pumakey.isKeyPositionPossible(t, key_position)) {
                String key = pumakey.getRecordKey(t, key_position);
                if (key.length() > maxKeyLength)
                    maxKeyLength = key.length();
            }
        }
        for (String t : dataset_2.collect()) {
            if (pumakey.isKeyPositionPossible(t, key_position)) {
                String key = pumakey.getRecordKey(t, key_position);
                if (key.length() > maxKeyLength)
                    maxKeyLength = key.length();
            }
        }
        System.out.println("\n\nMax key length: " + maxKeyLength + "\n\n");
        return maxKeyLength;
    }

    public static int getMaxKeyLength(int key_position, JavaRDD<String> dataset) {
        // Get the maximum length of the keys at the key position
        System.out.println("\n\nGet the maximum length of the keys at the key position \n\n");
        int maxKeyLength = 0;
        for (String t : dataset.collect()) {
            if (pumakey.isKeyPositionPossible(t, key_position)) {
                String key = pumakey.getRecordKey(t, key_position);
                if (key.length() > maxKeyLength)
                    maxKeyLength = key.length();
            }
        }
        System.out.println("\n\nMax key length: " + maxKeyLength + "\n\n");
        return maxKeyLength;
    }

    // BH1 Fuzzy self join
    // s input string
    // ms modified string
    // d
    // t thresolde distance to join on
    public static void generateBHBallTuple(char[] str, char[] ms, int length, int eps,
            ArrayList<Tuple2<String, Tuple2<String, String>>> out, String t, String key,
            ArrayList<String> vocabulary) throws IOException, InterruptedException {

        char[] local = ms.clone();
        if (length > 0 && eps > 0) // Go at the first letter of the string
            generateBHBallTuple(str, ms, length - 1, eps, out, t, key, vocabulary);

        if (eps > 0) {
            for (String vocValue : vocabulary) { // T,i,t,l,e,_,1
                if (!vocValue.equals(String.valueOf(str[length]))) {
                    local[length] = vocValue.charAt(0); // Copie vocValue : T,i,t,l,e,_,1
                    String tmp = String.valueOf(local); // remet en String l'entièreté de str modifié: Xitle_1
                    out.add(new Tuple2<String, Tuple2<String, String>>(tmp, new Tuple2<String, String>(key, t)));
                    if (length > 0 && eps > 1) // Generate all possible combinations (ex: d=3 => d=2 => d=1)
                        generateBHBallTuple(str, local, length - 1, eps - 1, out, t, key, vocabulary);
                }
            }
        }
    }

    public static void generateBHBall2way(char[] str, char[] ms, int length, int eps,
            ArrayList<Tuple2<String, String>> out, String t, String key, ArrayList<String> vocabulary)
            throws IOException, InterruptedException {

        char[] local = ms.clone();
        if (length > 0 && eps > 0) // Go at the first letter of the string
            generateBHBall2way(str, ms, length - 1, eps, out, t, key, vocabulary);

        if (eps > 0) {
            for (String vocValue : vocabulary) { // T,i,t,l,e,_,1
                if (!vocValue.equals(String.valueOf(str[length]))) {
                    local[length] = vocValue.charAt(0); // Copie vocValue : T,i,t,l,e,_,1
                    String tmp = String.valueOf(local); // remet en String l'entièreté de str modifié: Xitle_1
                    out.add(new Tuple2<String, String>(tmp, t));
                    if (length > 0 && eps > 1) // Generate all possible combinations (ex: d=3 => d=2 => d=1)
                        generateBHBall2way(str, local, length - 1, eps - 1, out, t, key, vocabulary);
                }
            }
        }
    }

    public static String[] getSplits(String str, int eps) {
        int b = str.length();
        int subS = (int) Math.ceil(b / (eps + 1.0));
        String[] keys = new String[eps + 1];

        int numMaxStrings = keys.length - (((eps + 1) * subS) - str.length());

        int offset = 0;
        for (int i = 0; i < keys.length; i++) {
            if (numMaxStrings > 0) {
                // add at beginning of keys[] strings of size subS
                keys[i] = str.substring(offset, Math.min(offset + subS, b));
                offset += subS;
                numMaxStrings--;
            } else {
                // add in end portions of keys[] strings of size (subS -1)
                keys[i] = str.substring(offset, Math.min(offset + subS - 1, b));
                offset += subS - 1;
            }
        } // end for

        /*
         * //output all key value pairs
         * for(int i = 0; i < keys.length; i++)
         * {
         * System.out.println("<" + keys[i] + ", " + i + ">, " + str);
         * }
         */
        return keys;
    }

    public static String[] getIndex_Splits(String str, int eps) {
        int b = str.length();
        int subS = (int) Math.ceil(b / (eps + 1.0));
        String[] keys = new String[eps + 1];

        int numMaxStrings = keys.length - (((eps + 1) * subS) - str.length());

        int offset = 0;
        for (int i = 0; i < keys.length; i++) {
            if (numMaxStrings > 0) {
                // add at beginning of keys[] strings of size subS
                keys[i] = String.valueOf(i).concat("_").concat(str.substring(offset, Math.min(offset + subS, b)));
                offset += subS;
                numMaxStrings--;
            } else {
                // add in end portions of keys[] strings of size (subS -1)
                keys[i] = String.valueOf(i).concat("_").concat(str.substring(offset, Math.min(offset + subS - 1, b)));
                offset += subS - 1;
            }
        }
        return keys;
    }

    public static String[] getIndex_Splits(String str, int eps, int maxKeyLength) {
        int b = str.length();
        if (b == 0)
            return new String[0];

        int subS = (int) Math.ceil(maxKeyLength / (eps + 1.0));
        String[] keys = new String[eps + 1];

        int offset = 0;
        for (int i = 0; i < keys.length; i++) {
            if (b > (subS + offset)) {
                // add at beginning of keys[] strings of size subS
                keys[i] = String.valueOf(i).concat("_").concat(str.substring(offset, Math.min(offset + subS, b)));
                offset += subS;
            } else if (b > offset) {
                // add in end portions of keys[] strings of size (subS -1)
                keys[i] = String.valueOf(i).concat("_").concat(str.substring(offset, b));
                offset += subS;
            }
        }
        return keys;
    }

    public static ArrayList<Integer> getIndexSplits(String str, int eps) {
        ArrayList<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < eps + 1; i++) {
            indexes.add(i, i);
        }

        return indexes;
    }

    public static String getKeyInt(int i) {
        String tmp;
        int l = String.valueOf(CONST_FILTERS.vectorsize).length();
        String key = new String();
        tmp = String.valueOf(i);
        for (int j = 0; j < l - tmp.length() - 1; j++) {
            key = key.concat("0");
        }
        key = key.concat(tmp);
        return key;
    }

    public static void main(String[] args) {
        /*
         * getSplits("0123456789", 1);
         * System.out.println(getStrKey(
         * "entryNum02740871885,entryNum28378665360,entryNum42673710121,entryNum53273668845,entryNum57262513737,entryNum58377762763,entryNum60075877051,entryNum60331660522,entryNum75488543137,entryNum77721630705",
         * 5));
         * System.out.println(get2Keys(
         * "entryNum02740871885,entryNum28378665360,entryNum42673710121,entryNum53273668845,entryNum57262513737,entryNum58377762763,entryNum60075877051,entryNum60331660522,entryNum75488543137,entryNum77721630705",
         * 5, 6));
         * System.out.println(Integer.MAX_VALUE);
         */
        // System.out.println(getStrKey("entryNum02740871885,entryNum28378665360,entryNum42673710121,entryNum53273668845,entryNum57262513737,entryNum58377762763,entryNum60075877051,entryNum60331660522,entryNum75488543137,entryNum77721630705",
        // 5));
        // System.out.println(replaceKey("entryNum02740871885,entryNum28378665360,entryNum42673710121,entryNum53273668845,entryNum57262513737,entryNum58377762763,entryNum60075877051,entryNum60331660522,entryNum75488543137,entryNum77721630705",
        // CONSTANTS.SECOND_COL, "S"));
        // System.out.println(replaceKey5("entryNum02740871885,entryNum28378665360,entryNum42673710121,entryNum53273668845,entryNum57262513737,entryNum58377762763,entryNum60075877051,entryNum60331660522,entryNum75488543137,entryNum77721630705",
        // CONST.SECOND_COL, "SSSSS"));
        // System.out.println(getHashCode("2740871885".toCharArray(), 9));
        // System.out.println(getCountKey("entryNum02740871885,entryNum28378665360,entryNum42673710121,entryNum53273668845,entryNum57262513737,entryNum58377762763,entryNum60075877051,entryNum60331660522,entryNum75488543137,entryNum77721630705",
        // 1));
        // System.out.println(getStrKey1("entryNum02740871885,entryNum28378665360,entryNum42673710121,entryNum53273668845,entryNum57262513737,entryNum58377762763,entryNum60075877051,entryNum60331660522,entryNum75488543137,entryNum77721630705",
        // 1));
        /*
         * ArrayList<String> slist = new ArrayList<String> ();
         * slist.add("1");
         * slist.add("2");
         * //System.out.println(slist.size());
         * ArrayList<String> tlist = new ArrayList<String> ();
         * tlist.addAll(slist);
         * tlist.remove(tlist.size()-1);
         * for(String s:slist) System.out.println(s);
         * for(String s:tlist) System.out.println(s);
         */
        String s = "0871885";
        String t = "8665360";
        s = s.substring(0, s.length() - 1);
        t = t.substring(0, t.length() - 1);

        System.out.println(pumakey.isSimilair(s, t, 6));
        System.out.println(getRecordKey(
                "entryNum02740871885,entryNum28378665360,entryNum42673710121,entryNum53273668845,entryNum57262513737,entryNum58377762763,entryNum60075877051,entryNum60331660522,entryNum75488543137,entryNum77721630705",
                1));

        String[] split = getIndex_Splits("866536", 4);
        for (String str : split)
            System.out.println(str);

        System.out.println(pumakey.isSimilair("010", "110", 0));
        System.out.println(pumakey.isSimilair("010", "110", 1));
    }

}
