package wordcounter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class WordFreq implements WritableComparable<WordFreq> {

	private Text word;
	private IntWritable frequency;
	
	public WordFreq() {
		this.word = new Text();
		this.frequency = new IntWritable(0);
	}
	
	public WordFreq(Text word, IntWritable frequency) {
		this.word = word;
		this.frequency = frequency;
	}

	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public IntWritable getFrequency() {
		return frequency;
	}

	public void setFrequency(IntWritable frequency) {
		this.frequency = frequency;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		word.readFields(arg0);
		frequency.readFields(arg0);
	}
	
	@Override
	public void write(DataOutput arg0) throws IOException {
		word.write(arg0);
		frequency.write(arg0);
	}
	
	@Override
	public int compareTo(WordFreq o) {
		return this.frequency.compareTo(o.getFrequency());
	}
	
	
}
