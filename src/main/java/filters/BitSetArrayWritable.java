package filters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class BitSetArrayWritable implements Writable {

	public ArrayWritable bitsetArrayWritable;
	
	public BitSetArrayWritable() {
		
		List<BitSetWritable> empty = Collections.<BitSetWritable> emptyList();		
		this.bitsetArrayWritable = new ArrayWritable(BitSetWritable.class, empty.toArray(new Writable[empty.size()]));

	}
	
	public BitSetArrayWritable(BitSet[] bitSets) {
		
		ArrayList<BitSetWritable> items = new ArrayList<BitSetWritable>();
		for (int i=0; i < bitSets.length; i++) {
			items.add(new BitSetWritable(bitSets[i]));
		}

		this.bitsetArrayWritable = new ArrayWritable(BitSetWritable.class, items.toArray(new Writable[items.size()]));

	}
	

	public void readFields(DataInput in) throws IOException {
		bitsetArrayWritable.readFields(in);
		
	}


	public void write(DataOutput out) throws IOException {
		bitsetArrayWritable.write(out);		
	}
	
	public BitSet[] get() {
		
		Writable[] writableArray = bitsetArrayWritable.get();
		
		ArrayList<BitSet> list = new ArrayList<BitSet>();
		for (int i=0; i < writableArray.length; i++) {
			
			BitSetWritable item = (BitSetWritable) writableArray[i];			
			list.add(item.get());
			
		}
		
		return list.toArray(new BitSet[list.size()]);
		
	}

}