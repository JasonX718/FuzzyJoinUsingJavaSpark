package filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.CopyOnWriteArrayList;

import Database.pumakey.pumakey;
import scala.Tuple2;

public class BallOfRadius {

    //public static final char[] ALPHABET2 = {'0','1','2','3','4','5','6','7','8','9'};
    //public static final char[] ALPHABET2 = {'a','b','c'};
    //public static final char[] ALPHABET2 = {'0','1'};
    //public static final char[] ALPHABET2 = CONSTANTS.ALPHABET2.toCharArray();

    public static int getDistanceBetween(char[] c1, char[] c2) {
        int dist = 0;
        for (int i = 0; i < c1.length; i++) {
            if (c1[i] != c2[i])
                dist++;
        }
        return dist;
    }

    static void generateBall(char[] s, char[] ms, int index, int d) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBall(s, ms, index - 1, d);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    if (String.valueOf(local).compareTo(String.valueOf(s)) < 0)
                        System.out.println("MapOut: " + String.valueOf(local));
                    else System.out.println(-1);
                    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));

                    if (index > 0 && d > 1)
                        generateBall(s, local, index - 1, d - 1);
                }
            }//end for loop

    }//end generateBall

    static void generateArrBall(char[] s, char[] ms, int index, int d, String[] arr) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateArrBall(s, ms, index - 1, d, arr);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    arr[Integer.valueOf(String.valueOf(local))] = String.valueOf(local);
                    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));

                    if (index > 0 && d > 1)
                        generateArrBall(s, local, index - 1, d - 1, arr);
                }
            }//end for loop

    }//end generateBall

    static void generateBallList(char[] s, char[] ms, int index, int d, ArrayList<Integer>[] list) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBallList(s, ms, index - 1, d, list);
        if (d > 0) {
            String si, sj;
            sj = String.valueOf(s);
            int indexi, indexj;
            indexj = Integer.valueOf(sj);
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    si = String.valueOf(local);
                    indexi = Integer.valueOf(si);
                    //		    if(indexi>indexj) {
                    //			    if(list[indexi]==null) list[indexi]=new ArrayList<>();
                    if (list[indexj] == null) list[indexj] = new ArrayList<>();
                    //			    list[indexi].add(indexj);
                    list[indexj].add(indexi);
                    //		    }

                    //this.hblist[convertB2I(String.valueOf(s))].set(convertB2I(String.valueOf(local)));

                    if (index > 0 && d > 1)
                        generateBallList(s, local, index - 1, d - 1, list);
                }
            }//end for loop
        }
    }//end generateBall

    public static void generateBallBF(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>> out, BloomFilter BF) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBallBF(s, ms, index - 1, d, out, BF);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    String tmp = String.valueOf(local);
                    if (BF.membershipTest(new Key(tmp.getBytes())))
                        out.add(new Tuple2<String, String>(tmp, String.valueOf(s)));
                    //BF.add(new Key(String.valueOf(local).getBytes()));
                    if (index > 0 && d > 1)
                        generateBallBF(s, local, index - 1, d - 1, out, BF);
                }
            }//end for loop

    }//end generateBall

    public static void generateBHBallBF(char[] s, char[] ms, int index, int d, CopyOnWriteArrayList<Tuple2<String, String>> out, BloomFilter BF, String t) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBHBallBF(s, ms, index - 1, d, out, BF, t);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    String tmp = String.valueOf(local);
                    if (BF.membershipTest(new Key(tmp.getBytes())))
                        //out.add(new Tuple2<String, String>(tmp, String.valueOf(s)));
                        out.add(new Tuple2<String, String>(tmp, t));
                    if (index > 0 && d > 1)
                        generateBHBallBF(s, local, index - 1, d - 1, out, BF, t);
                }
            }//end for loop

    }//end generateBall

    public static void generateBHBallBF(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>> out, BloomFilter BF, String t) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBHBallBF(s, ms, index - 1, d, out, BF, t);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    String tmp = String.valueOf(local);
                    if (BF.membershipTest(new Key(tmp.getBytes())))
                        //out.add(new Tuple2<String, String>(tmp, String.valueOf(s)));
                        out.add(new Tuple2<String, String>(tmp, t));
                    if (index > 0 && d > 1)
                        generateBHBallBF(s, local, index - 1, d - 1, out, BF, t);
                }
            }//end for loop

    }//end generateBall

    public static void generateBHBallBF(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>> out, BitSet BF, String t) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBHBallBF(s, ms, index - 1, d, out, BF, t);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (index > 0 && value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    String tmp = String.valueOf(local);
                    if (CONST_FILTERS.membershipTestBF(BF, tmp))
                        //out.add(new Tuple2<String, String>(tmp, String.valueOf(s)));
                        out.add(new Tuple2<String, String>(tmp, t));
                    if (index > 0 && d > 1)
                        generateBHBallBF(s, local, index - 1, d - 1, out, BF, t);
                }
            }//end for loop

    }//end generateBall

    /*public static boolean generateBHBallBFtest(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, Tuple2<String,String>>>  out, BitSet BF, Tuple2<String,String> t,boolean b) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException
    {
    //s input string
    //ms modified string
    //d
    //t thresolde distance to join on
    char[] local = ms.clone();
    if(index > 0 && d > 0)
        b=generateBHBallBFtest(s, ms, index - 1, d,out, BF,t, b);
    if(d>0)
    for(char value : CONST.ALPHABET)
        {
        if(index > 0 && value != s[index])
        {
            local[index] = value;
            //System.out.println("MapOut: " + String.valueOf(local) );
            String tmp = String.valueOf(local);
           // if(CONST_FILTERS.membershipTestBF(BF,tmp)) {
                if(tmp.compareTo(t._1) < 0)
                    //System.out.println(tmp + "_" + key);
                    out.add(new Tuple2<String, Tuple2<String,String>>(tmp, new Tuple2<String, String>(t._1, t._2)));
                else //System.out.println(b);
                    b=true;
            //}
            if(index > 0 && d > 1)
                b=generateBHBallBFtest(s, local, index - 1, d - 1,out, BF,t, b);
        }
        }//end for loop
    return b;
    }//end generateBall
*/
    //BFBH1 self join
    public static boolean generateBFBallTuple(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, Tuple2<String, String>>> out, String t, boolean b, String key, BitSet set) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            b = generateBFBallTuple(s, ms, index - 1, d, out, t, b, key, set);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    String tmp = String.valueOf(local);
                    if (CONST_FILTERS.membershipTestBF(set, tmp)) {
                        if (tmp.compareTo(key) < 0)
                            //System.out.println(tmp + "_" + key);
                            out.add(new Tuple2<String, Tuple2<String, String>>(tmp, new Tuple2<String, String>(key, t)));
                        else //System.out.println(b);
                            b = true;
                        //if(tmp.compareTo(key) > 0) b=true;
                    }
                    if (index > 0 && d > 1)
                        b = generateBFBallTuple(s, local, index - 1, d - 1, out, t, b, key, set);

                }
            }//end for loop
        return b;
    }//end generateBall

    //BFBH1 2way join
    public static void generateBFBall2way(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>> out, String t, String key, BitSet set) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBFBall2way(s, ms, index - 1, d, out, t, key, set);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    String tmp = String.valueOf(local);
                    if (CONST_FILTERS.membershipTestBF(set, tmp)) {
                        out.add(new Tuple2<String, String>(tmp, t));
                    }
                    if (index > 0 && d > 1)
                        generateBFBall2way(s, local, index - 1, d - 1, out, t, key, set);

                }
            }//end for loop
    }//end generateBall

    //BFBH1 2way join
    public static boolean generateBFBall2waytest(char[] s, char[] ms, int index, int d, BitSet set) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            return generateBFBall2waytest(s, ms, index - 1, d, set);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    String tmp = String.valueOf(local);
                    if (CONST_FILTERS.membershipTestBF(set, tmp)) {
                        return true;
                    }
                    if (index > 0 && d > 1)
                        return generateBFBall2waytest(s, local, index - 1, d - 1, set);

                }
            }//end for loop
        return false;
    }//end generateBall

    public static void generateBHBallBFtest(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, Tuple2<String, String>>> out, BitSet BF, Tuple2<String, String> t) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBHBallBFtest(s, ms, index - 1, d, out, BF, t);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (index > 0 && value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    String tmp = String.valueOf(local);
                    if (CONST_FILTERS.membershipTestBF(BF, tmp)) {
                        if (tmp.compareTo(t._1) < 0)
                            //System.out.println(tmp + "_" + key);
                            out.add(new Tuple2<String, Tuple2<String, String>>(tmp, new Tuple2<String, String>(t._1, t._2)));
                        //  else //System.out.println(b);
                        //  	b=true;
                    }
                    if (index > 0 && d > 1)
                        generateBHBallBFtest(s, local, index - 1, d - 1, out, BF, t);
                }
            }//end for loop
    }//end generateBall

    public static void generateBallSplitBFtest(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>> out, BitSet BF, String t, String[] splits, ArrayList<Integer> indexes, int eps) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException {

        char[] local = ms.clone();
        if (indexes.size() > 0 && index > 0 && d > 0)
            generateBallSplitBFtest(s, ms, index - 1, d, out, BF, t, splits, indexes, eps);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (indexes.size() > 0 && index > 0 && value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    String tmp = String.valueOf(local);
                    if (CONST_FILTERS.membershipTestBF(BF, tmp)) {
                        String[] tmpsplits = pumakey.getIndex_Splits(tmp, eps);
                        int i = 0;
                        for (int split : indexes) {
                            if (splits[split].compareTo(tmpsplits[split]) == 0) {
                                out.add(new Tuple2<String, String>(splits[split], t));
                                indexes.remove(i);
                                break;
                            }
                            i++;
                        }
                    }
                    if (indexes.size() > 0 && index > 0 && d > 1)
                        generateBallSplitBFtest(s, local, index - 1, d - 1, out, BF, t, splits, indexes, eps);
                }
            }//end for loop
    }//end generateBall

    public static void generateListBallBF(char[] s, char[] ms, int index, int d, ArrayList<String> out, BloomFilter BF) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateListBallBF(s, ms, index - 1, d, out, BF);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    String tmp = String.valueOf(local);
                    if (BF.membershipTest(new Key(tmp.getBytes())))
                        out.add(tmp);
                    //BF.add(new Key(String.valueOf(local).getBytes()));
                    if (index > 0 && d > 1)
                        generateListBallBF(s, local, index - 1, d - 1, out, BF);
                }
            }//end for loop

    }//end generateBall

    public static void generateListBallBF(char[] s, char[] ms, int index, int d, ArrayList<String> out, BitSet BF) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateListBallBF(s, ms, index - 1, d, out, BF);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    String tmp = String.valueOf(local);
                    if (CONST_FILTERS.membershipTestBF(BF, tmp))
                        out.add(tmp);
                    //BF.add(new Key(String.valueOf(local).getBytes()));
                    if (index > 0 && d > 1)
                        generateListBallBF(s, local, index - 1, d - 1, out, BF);
                }
            }//end for loop

    }//end generateBall

    public static void generateBallBF(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>> out) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBallBF(s, ms, index - 1, d, out);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    out.add(new Tuple2<String, String>(String.valueOf(local), String.valueOf(s)));
                    if (index > 0 && d > 1)
                        generateBallBF(s, local, index - 1, d - 1, out);
                }
            }//end for loop

    }//end generateBall

    public static void generateListBallBF(char[] s, char[] ms, int index, int d, ArrayList<String> out) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateListBallBF(s, ms, index - 1, d, out);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    out.add(String.valueOf(local));
                    if (index > 0 && d > 1)
                        generateListBallBF(s, local, index - 1, d - 1, out);
                }
            }//end for loop

    }//end generateBall

    public static void generateListBallBF(char[] s, char[] ms, int index, int d, CopyOnWriteArrayList<String> out) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateListBallBF(s, ms, index - 1, d, out);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    out.add(String.valueOf(local));
                    if (index > 0 && d > 1)
                        generateListBallBF(s, local, index - 1, d - 1, out);
                }
            }//end for loop

    }//end generateBall

    public static Boolean testListBallBF(char[] s, char[] ms, int index, int d, ArrayList<String> out, BloomFilter bf) throws IOException, InterruptedException {
        Boolean r = false;
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (r == false && index > 0 && d > 0)
            r = testListBallBF(s, ms, index - 1, d, out, bf);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    //out.add(String.valueOf(local));
                    if (bf.membershipTest(new Key(String.valueOf(local).getBytes())))
                        return true;
                    if (r == false && index > 0 && d > 1)
                        r = testListBallBF(s, local, index - 1, d - 1, out, bf);
                }
            }//end for loop

        return r;
    }//end generateBall

    public static void generateBallAcc2(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>> out, BloomFilter BF) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBallBF(s, ms, index - 1, d, out, BF);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    String tmpstr = String.valueOf(local);
                    if (BF.membershipTest(new Key(tmpstr.getBytes()))) ;
                    out.add(new Tuple2<String, String>(tmpstr, String.valueOf(s)));

                    if (index > 0 && d > 1)
                        generateBallBF(s, local, index - 1, d - 1, out, BF);
                }
            }//end for loop

    }//end generateBall


    public static void generateBall(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>> out) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBall(s, ms, index - 1, d, out);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    //System.out.println("MapOut: " + String.valueOf(local) );
                    out.add(new Tuple2<String, String>(String.valueOf(local), String.valueOf(s)));
                    if (index > 0 && d > 1)
                        generateBall(s, local, index - 1, d - 1, out);
                }
            }//end for loop

    }//end generateBall

    public static void generateBHBall(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>> out, String t) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBHBall(s, ms, index - 1, d, out, t);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    out.add(new Tuple2<String, String>(String.valueOf(local), t));
                    if (index > 0 && d > 1)
                        generateBHBall(s, local, index - 1, d - 1, out, t);
                }
            }//end for loop

    }//end generateBall

    public static void generateBHBall(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, Tuple2<String, String>>> out, String t, String tmp, boolean b, String key) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBHBall(s, ms, index - 1, d, out, t, tmp, b, key);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    tmp = String.valueOf(local);
                    if (tmp.compareTo(key) < 0)
                        out.add(new Tuple2<String, Tuple2<String, String>>(tmp, new Tuple2<String, String>(key, t)));
                    else b = true;
                    if (index > 0 && d > 1)
                        generateBHBall(s, local, index - 1, d - 1, out, t, tmp, b, key);
                }
            }//end for loop

    }//end generateBall

    public static boolean generateBHBalltest(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, Tuple2<String, String>>> out, String t, String tmp, boolean b, String key) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBHBalltest(s, ms, index - 1, d, out, t, tmp, b, key);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    tmp = String.valueOf(local);
                    if (tmp.compareTo(key) < 0)
                        //System.out.println(tmp + "_" + key);
                        out.add(new Tuple2<String, Tuple2<String, String>>(tmp, new Tuple2<String, String>(key, key)));
                    //else //System.out.println(b);
                    //	b=true;
                    //if(tmp.compareTo(key) > 0) b=true;
                    if (index > 0 && d > 1)
                        generateBHBalltest(s, local, index - 1, d - 1, out, t, tmp, b, key);
                }
            }//end for loop
        return b;
    }//end generateBall


    //BH1 Fuzzy self join
    public static boolean generateBHBallTuple(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, Tuple2<String, String>>> out, String t, boolean b, String key) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on

        char[] local = ms.clone();
        if (index > 0 && d > 0)
            b = generateBHBallTuple(s, ms, index - 1, d, out, t, b, key);
        if (d > 0) {
            for (String value : pumakey.vocabulary) {
                if(!value.equals(String.valueOf(s[index]))) {
                    local[index] = value.charAt(0);
                    String tmp = String.valueOf(local);
                    if (tmp.compareTo(key) < 0)
                        // Ball hashing add an element to out
                        out.add(new Tuple2<String, Tuple2<String, String>>(tmp, new Tuple2<String, String>(key, t)));
                    else
                        b = true;
                    if (index > 0 && d > 1) // Generate all possible combinations (ex: d=3 => d=2 => d=1)
                        b = generateBHBallTuple(s, local, index - 1, d - 1, out, t, b, key);
                }
            }
        }
        return b;
    }
        //BH1 Fuzzy self join
        public static boolean generateBHBallTuple(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, Tuple2<String, String>>> out, String t, boolean b, String key,  ArrayList<String> vocabulary) throws IOException, InterruptedException {
            //s input string
            //ms modified string
            //d
            //t thresolde distance to join on
    
            char[] local = ms.clone();
            if (index > 0 && d > 0)
                b = generateBHBallTuple(s, ms, index - 1, d, out, t, b, key, vocabulary);
            if (d > 0) {
                for (String value : vocabulary) {
                    if(!value.equals(String.valueOf(s[index]))) {
                        local[index] = value.charAt(0);
                        String tmp = String.valueOf(local);
                        if (tmp.compareTo(key) < 0)
                            // Ball hashing add an element to out
                            out.add(new Tuple2<String, Tuple2<String, String>>(tmp, new Tuple2<String, String>(key, t)));
                        else
                            b = true;
                        if (index > 0 && d > 1) // Generate all possible combinations (ex: d=3 => d=2 => d=1)
                            b = generateBHBallTuple(s, local, index - 1, d - 1, out, t, b, key, vocabulary);
                    }
                }
            }
            return b;
        }


    //BH1 Fuzzy 2way join
    public static void generateBHBall2way(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>>
            out, String t, String key,ArrayList<String> vocabulary) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBHBall2way(s, ms, index - 1, d, out, t, key, vocabulary);
        if (d > 0)
            for (String value : vocabulary) {
                if(!value.equals(String.valueOf(s[index]))) {
                    local[index] = value.charAt(0);
                    String tmp = String.valueOf(local);
                    out.add(new Tuple2<String, String>(tmp, t));
                    if (index > 0 && d > 1)
                        generateBHBall2way(s, local, index - 1, d - 1, out, t, key,vocabulary);
                }
            }
    }

    //BH1 Fuzzy 2way join
    public static void generateBHBall2way(char[] s, char[] ms, int index, int d, ArrayList<Tuple2<String, String>>
            out, String t, String key) throws IOException, InterruptedException {
        //s input string
        //ms modified string
        //d
        //t thresolde distance to join on
        char[] local = ms.clone();
        if (index > 0 && d > 0)
            generateBHBall2way(s, ms, index - 1, d, out, t, key);
        if (d > 0)
            for (char value : CONST.ALPHABET) {
                if (value != s[index]) {
                    local[index] = value;
                    String tmp = String.valueOf(local);
                    out.add(new Tuple2<String, String>(tmp, t));
                    if (index > 0 && d > 1)
                        generateBHBall2way(s, local, index - 1, d - 1, out, t, key);
                }
            }//end for loop
    }//end generateBall

    public static void main(String[] args) throws IOException, InterruptedException {
		/*char[] s = "12".toCharArray();
		generateBall(s,s,s.length-1,1);*/
/*		char[] s = "718513".toCharArray();
		String tmp="";
		boolean b = false;
		ArrayList<Tuple2<String, Tuple2<String,String>>>  out = new ArrayList<>();
		b=generateBHBalltest(s, s, s.length-1, 1, out, "718513", tmp, b, "718513");
		System.out.println(b);
*/		
/*		char[] s="000000".toCharArray();
		generateBall(s, s, s.length-1, 6);
*///		char[] c1, c2;
		
		/*c1 = "6 abc".toCharArray();
		c2 = "06 abc".toCharArray();
		c3 = "6 abcd".toCharArray();
		System.out.println("edit distance " + levenshteinDistance(c1, c2));
		System.out.println("edit distance " + levenshteinDistance(c1, c3));
		
		
		c1 = "abc".toCharArray();
		c2 = "0bc".toCharArray();
		System.out.println("edit distance " + getDistanceBetween(c1, c2));
		*/


//		c1="abce".toCharArray();
//		c2="0bca".toCharArray();
//		System.out.println("Hamming distance " + getDistanceBetween(c1, c2));
//		generateBall(c1, c1.length-1, 2);
		
		/*BitSet bs = new BitSet(4);
		bs.set(1);
		bs.set(3);
		//System.out.println(bs.toString());
		for(int i=0;i<bs.length();i++) System.out.println(bs.get(i));*/
/*		BitSet[] b = new BitSet[1000000]; //outofmemory
		for(int i=0; i<b.length;i++) {
			b[i]=new BitSet(1000000);
			b[i].set(999999);
		}
		System.out.println(b);*/
/*		char[] s="000000".toCharArray();
		String[] arr = new String[1000000];
		arr[0]="000000";
		//int[] indexes = new int[1000000];
		generateArrBall(s, s, 5, 6, arr);
		//for(String t:arr) System.out.println(t);
		System.out.println("finish ball generation");
		ArrayList<Integer>[] ball = new ArrayList[1000000];
		for(int i=0;i<arr.length;i++) {
			generateBallList(s, s, 5, 1, ball);
		}
		System.out.println("finish filter generation");
*/
        //char[] s="662043".toCharArray();
        //generateBall(s, s, 5, 1);
        ArrayList<Integer> arr = new ArrayList<>();
        arr.add(0, 0);
        arr.add(2, 2);
        for (int i : arr)
            System.out.println(i);
        //System.out.println("362043".compareTo(String.valueOf(s)));
		
/*		BitSet b = new BitSet(6);
		b.set(2);
		b.set(5);
		String str=b.toString();
		str=str.substring(1,str.length()-1);
		System.out.println(str);
		String[] arr = str.split(", ");
		for(String tmp:arr) {
			System.out.println(Integer.valueOf(tmp));
		}
*/
    }

}
