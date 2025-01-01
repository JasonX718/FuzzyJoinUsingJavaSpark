package filters;

import org.apache.spark.HashPartitioner;
import org.apache.spark.storage.StorageLevel;

public class CONST {
//	public static final String MASTER = "yarn";
	//public static final String MASTER = "spark://quyen-master:7077";

	public static final String NAMENODE = "127.0.0.1";
	//public static final int NUM_EXECUTORS = 57;
	//public static final int NUM_EXECUTORS = 14;

	public static final String HADOOP_TMPDIR = "/opt/hadoop/tmp";

	/**
	 * number of columns
	 */
//	public static final int NUM_COL=40;
	/**
	 * Symbol for split a line into Arrays.
	 */
	public static final String SEPARATOR = ",";
	
	/**
	 * Column want to join.
	 */
//	public static final int FIRST_COL = 29, SECOND_COL = 26;

	public static final int FIRST_COL = 0, SECOND_COL = 0; // BASE = 36 AND 38
	//public static final int LENGTH = 11; //to calcul ball space
	public static final int KEY_MINI_LENGTH = 0; //to calcul ball space : BASE = 6
	public static final int LENGTH1 = 7;

	public static final int NUM_PARTITION = 14;
	//public static final int NUM_PARTITION = 14;
	//public static final int NUM_PARTITION = 10;

	
	public static final StorageLevel STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK_SER();
	//public static final StorageLevel STORAGE_LEVEL = StorageLevel.MEMORY_ONLY();
	//public static final HashPartitioner partition = new HashPartitioner(CONSTANTS.NUM_PARTITION);
	public static final HashPartitioner partition = new HashPartitioner(560);

	//public static final String ALPHABET2 = "Z, A, C,  , d, e, T, y, D, o, u, a, r, ., l, Ã©, L, i, s, J, v, t, 7, 4, n, M, c, h, 1, 3, z, 0, Q, p, N, 9, R, Ã‰, b, Ã¨, K, m, q, 2, G, P, 5, 8, H, B, f, E, \", 6, V, S, ', g, x, -, F, I, O, Ã´, w, (, ), Ã , Ãª, Y, ÃŽ, k, Ã§, Ã®, U, Â°, Ã«, j, /, &, @, :, W, X, Ã¢, â€“, Ã€, â€™, >, Ã», Ã‚, Ãˆ, â€œ, â€�, Ã¯, Â´, Ã£, â€¹, Ã¼, ;, â€°, Å“, Ã¶, Ã³, ï¿½, ?, Ã¹"; //107
	//public static final String ALPHABET2 = "z, a, c,  , d, e, t, y, o, u, r, ., l, é, i, s, j, v, 7, 4, n, m, h, 1, 3, 0, q, p, 9, b, è, k, 2, g, 5, 8, f, 6, ', x, -, ,, w, (, ), à, ê, î, ç, °, ë, ô, /, â, @, &, –, ’, >, \", ï, û, “, ”, œ, ´, ʼ, ã, ‹, ü, :, ;, ‰, ö, ó, ñ, �, ?, ù, º"; //81
//	public static final char[] ALPHABET = {'0','1'};
	public static final char[] ALPHABET = {'0','1', '2', '3','4','5','6','7','8','9'};
	
}
