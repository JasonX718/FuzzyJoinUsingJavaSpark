package Database.pumakey;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
//import java.util.ArrayList;
import java.util.Random;

import filters.CONST;
import filters.CONST_FILTERS;

public class pumakey {
    public static ArrayList<String> vocabulary = new ArrayList<String>();

    public static int getRandomCode(int j) {
        /*
         * int code = 0;
         *
         * for (int i = 0; i < str.length; i++) { code += str[i] * 31^(i); }
         */
        Random r = new Random();
        int code = Math.abs(r.nextInt());
        return code % j;
    }

    public static int getHashCode(char[] str, int j) {

        int code = 0;
        for (int i = 0; i < str.length; i++) {
            code += str[i] * 31 ^ (i);
        }
        // System.out.println("str : "+ str[0] + str[1]+str[2]+str[3]+" code = " + code
        // + " % " + j + " = " + code % j);

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

        String[] splits = str.split(",");
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
            String[] splits = dataset.split(",");
            if(!splits[keyCol].equals("null"))
                return splits[keyCol];
            else
                return "";
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
    public static int getDistance(String str1, String str2) {
        int dist = 0;
        for (int i = 0; i < str1.length(); i++) {
            if (str1.charAt(i) != str2.charAt(i))
                dist++;
        }
        return dist;
    }

    public static boolean isSimilair(String str1, String str2, int d) {
        int dist = 0, str1Length = str1.length() - 1;
        if ((str1.length() == 0) || (str2.length() == 0))
            return false;
        else {
            while (dist <= d && str1Length >= 0) {
                if (str1.charAt(str1Length) != str2.charAt(str1Length))
                    dist++;
                str1Length--;
            }
            if (dist <= d)
                return true;
            else
                return false;
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

    public static ArrayList<Integer> getIndexSplits(String str, int eps) {
        ArrayList<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < eps + 1; i++) {
            indexes.add(i, i);
        } // end for

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
