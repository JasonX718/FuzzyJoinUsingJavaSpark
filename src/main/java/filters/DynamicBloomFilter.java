/**
 *
 * Copyright (c) 2005, European Commission project OneLab under contract 034819 (http://www.one-lab.org)
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or 
 * without modification, are permitted provided that the following 
 * conditions are met:
 *  - Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in 
 *    the documentation and/or other materials provided with the distribution.
 *  - Neither the name of the University Catholique de Louvain - UCL
 *    nor the names of its contributors may be used to endorse or 
 *    promote products derived from this software without specific prior 
 *    written permission.
 *    
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package filters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
//import join.filter_recursive.CONSTANTS_FILTERS;
import org.apache.hadoop.conf.Configuration;

/**
 * Implements a <i>dynamic Bloom filter</i>, as defined in the INFOCOM 2006 paper.
 * <p>
 * A dynamic Bloom filter (DBF) makes use of a <code>s * m</code> bit matrix but
 * each of the <code>s</code> rows is a standard Bloom filter. The creation 
 * process of a DBF is iterative. At the start, the DBF is a <code>1 * m</code>
 * bit matrix, i.e., it is composed of a single standard Bloom filter.
 * It assumes that <code>n<sub>r</sub></code> elements are recorded in the 
 * initial bit vector, where <code>n<sub>r</sub> <= n</code> (<code>n</code> is
 * the cardinality of the set <code>A</code> to record in the filter).  
 * <p>
 * As the size of <code>A</code> grows during the execution of the application,
 * several keys must be inserted in the DBF.  When inserting a key into the DBF,
 * one must first get an active Bloom filter in the matrix.  A Bloom filter is
 * active when the number of recorded keys, <code>n<sub>r</sub></code>, is 
 * strictly less than the current cardinality of <code>A</code>, <code>n</code>.
 * If an active Bloom filter is found, the key is inserted and 
 * <code>n<sub>r</sub></code> is incremented by one. On the other hand, if there
 * is no active Bloom filter, a new one is created (i.e., a new row is added to
 * the matrix) according to the current size of <code>A</code> and the element
 * is added in this new Bloom filter and the <code>n<sub>r</sub></code> value of
 * this new Bloom filter is set to one.  A given key is said to belong to the
 * DBF if the <code>k</code> positions are set to one in one of the matrix rows.
 * <p>
 * Originally created by
 * <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 *
 * @see Filter The general behavior of a filter
 * @see BloomFilter A Bloom filter
 * 
 * @see <a href="http://www.cse.fau.edu/~jie/research/publications/Publication_files/infocom2006.pdf">Theory and Network Applications of Dynamic Bloom Filters</a>
 */
public class DynamicBloomFilter extends Filter {
  /** 
   * Threshold for the maximum number of key to record in a dynamic Bloom filter row.
   */
  private int thresholdKeysRow;

  /**
   * The number of keys recorded in the current standard active Bloom filter.
   */
  private int currentNbKeys;

  /**
   * The matrix of Bloom filter.
   */
  private BloomFilter[] matrix;

  /**
   * Zero-args constructor for the serialization.
   */
  public DynamicBloomFilter() { }
  
  public DynamicBloomFilter(int bloomSize) {
	  vectorSize = bloomSize;
  }

  /**
   * Constructor.
   * <p>
   * Builds an empty Dynamic Bloom filter.
   * @param vectorSize The number of bits in the vector.
   * @param nbHash The number of hash function to consider.
   * @param hashType type of the hashing function (see
   * {@link org.apache.hadoop.util.hash.Hash}).
   * @param thresKeysPerBF The threshold for the maximum number of keys to record in a
   * dynamic Bloom filter row.
   */
  public DynamicBloomFilter(int vectorSize, int nbHash, int hashType, int thresKeysPerBF) {
    super(vectorSize, nbHash, hashType);

    this.thresholdKeysRow = thresKeysPerBF;
    this.currentNbKeys = 0;

    matrix = new BloomFilter[1];
    matrix[0] = new BloomFilter(this.vectorSize, this.nbHash, this.hashType);
  }

  //Quyen's add function
  public DynamicBloomFilter(DynamicBloomFilter copy) {
	  super(copy.matrix[0].vectorSize, copy.matrix[0].nbHash, copy.matrix[0].hashType);
	  this.thresholdKeysRow = copy.thresholdKeysRow;
	  this.currentNbKeys = copy.currentNbKeys;
	  this.matrix = new BloomFilter[copy.matrix.length];
	  /*for (int i=0; i<matrix.length; i++) {
		  this.matrix[i] = new BloomFilter(copy.matrix[i]);
	  }*/
	  System.arraycopy(this.matrix, 0, copy.matrix, 0, this.matrix.length);
  }
  
  @Override
  public void add(Key key) {
    if (key == null) {
      throw new NullPointerException("Key can not be null");
    }

    BloomFilter bf = getActiveStandardBF();

    if (bf == null) {
      addRow();
      bf = matrix[matrix.length - 1];
      currentNbKeys = 0;
    }

    bf.add(key);

    currentNbKeys++;
  }

  /**Not useful, need to use Counting Bloom Filter for find each key's position...
   * modified
   * Quyen
   */
  @Override
  public void and(Filter filter) {
    if (filter == null
        || !(filter instanceof DynamicBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }

    DynamicBloomFilter dbf = (DynamicBloomFilter)filter;

    /*if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }

    for (int i = 0; i < matrix.length; i++) {
      matrix[i].and(dbf.matrix[i]);
    }*/
    
    if (this.matrix.length >= dbf.matrix.length) {
    	BloomFilter bfMatrixTmp[] = new BloomFilter[dbf.matrix.length];
    	for (int i = 0; i < dbf.matrix.length; i++) {
            this.matrix[i].and(dbf.matrix[i]);
            bfMatrixTmp[i] = this.matrix[i];
          }
    	this.matrix = bfMatrixTmp;
    } else {
    	for (int i = 0; i < this.matrix.length; i++) {
            this.matrix[i].and(dbf.matrix[i]);
          }
    	currentNbKeys = thresholdKeysRow;
    }
  }

  @Override
  public boolean membershipTest(Key key) {
    if (key == null) {
      return true;
    }
    /*String str = new String(key.getBytes());
    System.out.println("Key test membership " + str);*/
    for (int i = 0; i < matrix.length; i++) {
      if (matrix[i].membershipTest(key)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void not() {
    for (int i = 0; i < matrix.length; i++) {
      matrix[i].not();
    }
  }

  /**
   * modified
   * Quyen
   */
  @Override
  public void or(Filter filter) {
    if (filter == null
        || !(filter instanceof DynamicBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }

    DynamicBloomFilter dbf = (DynamicBloomFilter)filter;

    /*if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
      throw new IllegalArgumentException("filters dbf cannot be or-ed" + dbf.matrix.length 
    		  + "|" + this.matrix.length + "|" + dbf.nr + "|" + this.nr);
    }*/
    
    /*if (this.matrix.length >= dbf.matrix.length) {
    	for (int i = 0; i < dbf.matrix.length; i++) {
            this.matrix[i].or(dbf.matrix[i]);
          }
    } else {
    	BloomFilter bfMatrixTmp[] = new BloomFilter[dbf.matrix.length];
    	for (int i = 0; i < this.matrix.length; i++) {
            this.matrix[i].or(dbf.matrix[i]);
            //bfMatrix[i] = new BloomFilter(this.matrix[i]);
            bfMatrixTmp[i] = this.matrix[i];
          }
    	for (int i = this.matrix.length; i < dbf.matrix.length; i++) {
    		bfMatrixTmp[i] = new BloomFilter(dbf.matrix[i]);
    		//this.matrix[i] = new BloomFilter(dbf.matrix[i]);
    	}
    	this.matrix = bfMatrixTmp;
    	currentNbKeys = thresholdKeysRow;
    }*/

    int sumNumKeys = this.currentNbKeys + dbf.currentNbKeys;
//    System.out.println(this.currentNbKeys + " " + dbf.currentNbKeys + " " + "sum num of keys " + sumNumKeys + " " + this.thresholdKeysRow);
    Boolean or_able = (sumNumKeys >= this.thresholdKeysRow) ? false : true;
    Boolean thisLarger = (this.currentNbKeys > dbf.currentNbKeys) ? true : false;
    BloomFilter bfMatrixTmp[] = new BloomFilter[ or_able 
                                                 ? (this.matrix.length + dbf.matrix.length - 1)
                                                 : (this.matrix.length + dbf.matrix.length)];
//    System.out.println(this.matrix.length + " " + dbf.matrix.length);
//    System.out.println("leng union " + bfMatrixTmp.length);
    /*for(int i = 0; i < this.matrix.length; i++) {
    	bfMatrixTmp[i] = this.matrix[i];
    }
    for(int i = 0; i <  dbf.matrix.length; i++) {
    	bfMatrixTmp[i+this.matrix.length] = dbf.matrix[i];
    }*/
    System.arraycopy(this.matrix, 0, bfMatrixTmp, 0, this.matrix.length-1);
    System.arraycopy(dbf.matrix, 0, bfMatrixTmp, this.matrix.length-1, dbf.matrix.length-1);
    if(or_able) { //2 final BF is small, can be or-ed
    	this.matrix[this.matrix.length-1].or(dbf.matrix[dbf.matrix.length-1]);
    	System.arraycopy(this.matrix, this.matrix.length - 1, bfMatrixTmp, bfMatrixTmp.length-1, 1);
    	this.currentNbKeys = sumNumKeys;
    } else {
    	if (thisLarger) { //BF final of this is lager than bf final of dbf --> set smallest BF is in final 
    		System.arraycopy(this.matrix, this.matrix.length-1, bfMatrixTmp, bfMatrixTmp.length-2, 1);
    		System.arraycopy(dbf.matrix, dbf.matrix.length-1, bfMatrixTmp, bfMatrixTmp.length-1, 1);
    		this.currentNbKeys = dbf.currentNbKeys;
    	} else {
    		System.arraycopy(dbf.matrix, dbf.matrix.length-1, bfMatrixTmp, bfMatrixTmp.length-2, 1);
    		System.arraycopy(this.matrix, this.matrix.length-1, bfMatrixTmp, bfMatrixTmp.length-1, 1); 
    	}
    }
    this.matrix = bfMatrixTmp;
    
  }

  //Not finish yet
  @Override
  public void xor(Filter filter) {
    if (filter == null
        || !(filter instanceof DynamicBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be xor-ed");
    }
    DynamicBloomFilter dbf = (DynamicBloomFilter)filter;

    if (dbf.matrix.length != this.matrix.length || dbf.thresholdKeysRow != this.thresholdKeysRow) {
      throw new IllegalArgumentException("filters cannot be xor-ed");
    }

    for(int i = 0; i<matrix.length; i++) {
        matrix[i].xor(dbf.matrix[i]);
    }
  }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder();

    for (int i = 0; i < matrix.length; i++) {
      res.append(matrix[i]);
      res.append(Character.LINE_SEPARATOR);
    }
    return res.toString();
  }

  // Writable

  @Override
  public void write(DataOutput out) throws IOException {
//	  System.out.println("\n >> dbf length: " + matrix.length );
	super.write(out);
    out.writeInt(thresholdKeysRow);
    out.writeInt(currentNbKeys);
    out.writeInt(matrix.length);
    for (int i = 0; i < matrix.length; i++) {
/*    	if (matrix[i].isEmpty() || matrix[i]==null) {
    		//System.out.println("\n >> dbf : [" + i + "] " + matrix[i] );
    		System.out.println("\n >> dbf empty: " + i );
    	}*/
    	matrix[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    thresholdKeysRow = in.readInt();
    currentNbKeys = in.readInt();
    /*if (matrix!=null) {
    	for (int i = 0; i< matrix.length; i++) {
    		matrix[i] = null;
    	}
    	matrix = null;
    }*/
    
    int len = in.readInt();
    
//    System.out.println("read dbf leng " + len);
    
    matrix = new BloomFilter[len];
    for (int i = 0; i < matrix.length; i++) {
      matrix[i] = new BloomFilter();
      //matrix[i] = new BloomFilter(CONSTANTS_FILTERS.SizeBF);
      matrix[i].readFields(in);
    }
  }

  /**
   * Adds a new row to <i>this</i> dynamic Bloom filter.
   */
  private void addRow() {
    BloomFilter[] tmp = new BloomFilter[matrix.length + 1];

    /*for (int i = 0; i < matrix.length; i++) {
      tmp[i] = matrix[i];
    }*/
    System.arraycopy(this.matrix, 0, tmp, 0, this.matrix.length);

    tmp[tmp.length-1] = new BloomFilter(vectorSize, nbHash, hashType);

    matrix = tmp;
    currentNbKeys = 0;
  }

  /**
   * Returns the active standard Bloom filter in <i>this</i> dynamic Bloom filter.
   * @return BloomFilter The active standard Bloom filter.
   * 			 <code>Null</code> otherwise.
   */
  private BloomFilter getActiveStandardBF() {
    if (currentNbKeys >= thresholdKeysRow) {
      return null;
    }

    return matrix[matrix.length - 1];
  }
  
	//Compress and save the difference filter to a file.bloom.gz on HDFS
	public void compressAndSaveFilterToHDFS(String hdfsCompressionFilePathName, Configuration conf) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//String hdfsUri = "hdfs://localhost:9000/"; String hdfsUri = "file:/";		
		//String hdfsCompressionFilePathName = BLOOMFILTER_PATH + "/" + bloomName + ".bloom.gz";
//		System.out.println("\n >> compressAndSaveFilter(), Hdfs compressioned filter file=" + hdfsCompressionFilePathName);

		//Creating a HDFS compression DataOutputStream associated with a (hdfs_file.codec, conf)
		FileCompressor fileCompressor = new FileCompressor();
		DataOutputStream hdfsCompOutStream = fileCompressor.createHdfsCompOutStream(hdfsCompressionFilePathName, conf);

		//Saving the bloom filter to the HDFS compression DataOutputStream
		this.write(hdfsCompOutStream);

		//Closing the HDFS compression DataOutputStream
		fileCompressor.close();
		hdfsCompOutStream.close();
	}

  
  public boolean isEmpty() {
	  if (this!=null) {
		  for (int i=0; i<matrix.length; i++) {
			  if(matrix[i]!=null && !matrix[i].isEmpty()) return false;
		  }
	  }
	  return true;
  }
  
  public int getLength() {
	  return matrix.length;
  }
  
  /**
   *  Cut out from begin to position of DBF
   * Use to OR Function for trust difference DBF
   * @param position
   * @author tttquyen
   */
  public void cutElementsFromPosition(int position) {
	  int num = this.matrix.length - position;
	  BloomFilter bfTmp[] = new BloomFilter[num];
	  System.arraycopy(this.matrix, position, bfTmp, 0, num);
	  this.matrix = bfTmp;
  }
}
