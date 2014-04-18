package wordcounter;

import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class Toper {

	public static class ToperMapper extends
			Mapper<TextPair, IntWritable, Text, WordFreq> {

		// type cle input, type valeur input, type clé output, type valeur
		// output

		public void map(TextPair cle, IntWritable valeur, Context contexte)
				throws IOException, InterruptedException {
			WordFreq wf1 = new WordFreq(cle.getFirst(), valeur);
			WordFreq wf2 = new WordFreq(cle.getSecond(), valeur);
			contexte.write(cle.getFirst(), wf2);
			contexte.write(cle.getSecond(), wf1);
		}
	}

	public static class ToperReducer extends
			Reducer<Text, WordFreq, Text, TopFive> {

		// type clé input, type valeur input, type clé output, type valeur
		// output
		public void reduce(Text cle, Iterable<WordFreq> valeurs,
				Context contexte) throws IOException, InterruptedException {
			WordFreq[] top = new WordFreq[5];
			for(int i=0;i<top.length;i++)
				top[i]=new WordFreq();
			TopFive result = new TopFive(WordFreq.class);
			for(WordFreq valeur : valeurs){
				WordFreq v = WritableUtils.clone(valeur, contexte.getConfiguration());
				insert(top, v);
			}
			result.set(top);
			contexte.write(cle, result);
		}

		private void insert(WordFreq[] top, WordFreq v) {
			int i=0;
			int cmp=-1;
			while(i<top.length && cmp<0){
				cmp=v.compareTo(top[i]);
				if (cmp<0) {
					i++;
				}
			}
			WordFreq prev = v;
			while(i<top.length){
				WordFreq tmp = top[i];
				top[i]=prev;
				prev=tmp;
				i++;
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = new Job();
		job.setJarByClass(Toper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// peut être appelé plusieurs fois pour inputs multiples
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(ToperMapper.class);
//		job.setCombinerClass(ToperReducer.class);
		job.setReducerClass(ToperReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordFreq.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TopFive.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
