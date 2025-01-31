/**
 *
 * Copyright (c) 2005, European Commission project OneLab under contract 034819
 * (http://www.one-lab.org)
 * 
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
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;

/**
 * Defines the general behavior of a filter.
 * <p>
 * A filter is a data structure which aims at offering a lossy summary of a set <code>A</code>.  The
 * key idea is to map entries of <code>A</code> (also called <i>keys</i>) into several positions 
 * in a vector through the use of several hash functions.
 * <p>
 * Typically, a filter will be implemented as a Bloom filter (or a Bloom filter extension).
 * <p>
 * It must be extended in order to define the real behavior.
 * 
 * @see Key The general behavior of a key
 * @see HashFunction A hash function
 */
public abstract class LongFilter implements Writable {
  private static final int VERSION = -1; // negative to accommodate for old format 
  /** The vector size of <i>this</i> filter. */
  protected long vectorSize;

  /** The hash function used to map a key to several positions in the vector. */
  protected LongHashFunction hash;

  
  /** Type of hashing function to use. */
  protected int hashType;

  protected LongFilter() {}
  
  /** 
   * Constructor.
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash functions to consider.
   * @param hashType type of the hashing function (see {@link Hash}).
   */
  protected LongFilter(long vectorSize, int hashType) {
    this.vectorSize = vectorSize;
    this.hashType = hashType;
    this.hash = new LongHashFunction(this.vectorSize, this.hashType);
  }

  /**
   * Adds a key to <i>this</i> filter.
   * @param key The key to add.
   */
  public abstract void add(Key key);

  /**
   * Determines wether a specified key belongs to <i>this</i> filter.
   * @param key The key to test.
   * @return boolean True if the specified key belongs to <i>this</i> filter.
   * 		     False otherwise.
   */
  public abstract boolean membershipTest(Key key);

  /**
   * Peforms a logical AND between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   * @param filter The filter to AND with.
   */
  public abstract void and(LongFilter filter);

  /**
   * Peforms a logical OR between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   * @param filter The filter to OR with.
   */
  public abstract void or(LongFilter filter);

  /**
   * Peforms a logical XOR between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   * @param filter The filter to XOR with.
   */
  public abstract void xor(LongFilter filter);

  /**
   * Performs a logical NOT on <i>this</i> filter.
   * <p>
   * The result is assigned to <i>this</i> filter.
   */
  public abstract void not();

  /**
   * Adds a list of keys to <i>this</i> filter.
   * @param keys The list of keys.
   */
  public void add(List<Key> keys){
    if(keys == null) {
      throw new IllegalArgumentException("ArrayList<Key> may not be null");
    }

    for(Key key: keys) {
      add(key);
    }
  }//end add()

  /**
   * Adds a collection of keys to <i>this</i> filter.
   * @param keys The collection of keys.
   */
  public void add(Collection<Key> keys){
    if(keys == null) {
      throw new IllegalArgumentException("Collection<Key> may not be null");
    }
    for(Key key: keys) {
      add(key);
    }
  }//end add()

  /**
   * Adds an array of keys to <i>this</i> filter.
   * @param keys The array of keys.
   */
  public void add(Key[] keys){
    if(keys == null) {
      throw new IllegalArgumentException("Key[] may not be null");
    }
    for(int i = 0; i < keys.length; i++) {
      add(keys[i]);
    }
  }//end add()
  
  // Writable interface
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(VERSION);
    out.writeByte(this.hashType);
    out.writeLong(this.vectorSize);
  }

  public void readFields(DataInput in) throws IOException {
    int ver = in.readInt();
    if (ver > 0) { // old unversioned format
      //this.hashType = Hash.JENKINS_HASH;
    	this.hashType = Hash.MURMUR_HASH;
    } else if (ver == VERSION) {
    	this.hashType = in.readByte();
    } else {
    	throw new IOException("Unsupported version: " + ver);
    }
    this.vectorSize = in.readLong();
    this.hash = new LongHashFunction(this.vectorSize, this.hashType);
  }
}//end class
