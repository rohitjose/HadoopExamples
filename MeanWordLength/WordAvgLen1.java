package comp9313.ass1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static IntWritable length = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
		while (itr.hasMoreTokens()) {
			String current_word = itr.nextToken().toLowerCase();
			String word_character = String.valueOf(current_word.charAt(0));
			if (Character.isLetter(word_character.charAt(0))) {
				word.set(word_character);
				length = new IntWritable(current_word.length());
				context.write(word, length);
			}
			System.out.println(word.toString());

			Log log = LogFactory.getLog(TokenizerMapper.class);
			log.info("Mylog@Mapper: " + word.toString());
		}
	}
}

class DoubleSumReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int word_length_sum = 0;
		int word_count = 0;
		for (IntWritable val : values) {
			word_length_sum += val.get();
			word_count++;
		}

		result.set((word_length_sum / (word_count * 1.0)));
		context.write(key, result);

		Log log = LogFactory.getLog(DoubleSumReducer.class);
		log.info("Mylog@Reducer: " + key.toString() + " " + result.toString());

		System.out.println(key.toString() + " " + result.toString());
	}
}

public class WordAvgLen1 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordAvgLen1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(DoubleSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
