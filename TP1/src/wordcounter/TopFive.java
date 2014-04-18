package wordcounter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class TopFive  extends ArrayWritable  {

	public TopFive(Class<? extends Writable> valueClass) {
		super(valueClass);
	}

	@Override
	public String[] toStrings() {
		return super.toStrings();
	}

	@Override
	public String toString() {
		String str = "";
		WordFreq[] wr = (WordFreq[]) this.get();
		for(int i=0; i<wr.length; i++)
			str = str.concat(wr[i].getWord()+"["+wr[i].getFrequency()+"], ");
		return str;
	}
	
	
}
