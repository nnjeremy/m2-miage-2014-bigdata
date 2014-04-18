package wordcounter;

import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class WordCount {

	public static enum COUNTERS {
	  REQUETE_LONGUE
	}

	public static class WordCountMapper extends
			Mapper<LongWritable, Text, TextPair, IntWritable> {

		// type clé input, type valeur input, type clé output, type valeur
		// output

		public void map(LongWritable cle, Text valeur, Context contexte)
				throws IOException, InterruptedException {

			String ligne = valeur.toString();
			String[] requete = ligne.split("\t")[1].split(" ");
//			StringTokenizer tokenizer = new StringTokenizer(split[1]);
			if (requete.length>3)
				contexte.getCounter(COUNTERS.REQUETE_LONGUE).increment(1);
			Arrays.sort(requete);
			for (int i = 0; i < requete.length; i++) {
				for (int j = i + 1; j < requete.length; j++) {
					contexte.write(new TextPair(new Text(requete[i]), new Text(
							requete[j])), new IntWritable(1));
				}
			}
		}
	}

	public static class WordCountReducer extends
			Reducer<TextPair, IntWritable, TextPair, IntWritable> {

		// type clé input, type valeur input, type clé output, type valeur
		// output
		public void reduce(TextPair couple, Iterable<IntWritable> valeurs,
				Context contexte) throws IOException, InterruptedException {

			int somme = 0;
			for (IntWritable valeur : valeurs) {
				somme += valeur.get();
			}
			if (somme>3) {
				contexte.write(couple, new IntWritable(somme));	
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = new Job();
		job.setJarByClass(WordCount.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// peut être appelé plusieurs fois pour inputs multiples
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
