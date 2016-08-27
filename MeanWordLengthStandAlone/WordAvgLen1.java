package comp9313.ass1;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

/**
 * Class - Mapper Class
 */
class TokenizerMapperV1 extends Mapper<Object, Text, Text, IntWritable> {

	private static IntWritable length = new IntWritable(1);
	private Text word = new Text();

	/**
	 * This method organizes the data in the input files for the reducer phase.
	 * The data in the input file is tokenized into words and emitted to the
	 * reducer with the first character of the word as the key
	 * 
	 * @param key
	 *            - line offset
	 * @param value
	 *            - input text
	 * @param context
	 *            - allows the Mapper/Reducer to interact with the rest of the
	 *            Hadoop system
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// tokenize the sentence to words
		StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
		while (itr.hasMoreTokens()) {
			// lower-case conversion
			String current_word = itr.nextToken().toLowerCase();
			// map the first character
			String word_character = String.valueOf(current_word.charAt(0));
			if (isCharacter(word_character)) {// Check for
												// alphabets
				word.set(word_character);
				length = new IntWritable(current_word.length());
				context.write(word, length);
			}

			// Mapper Logging
			Log log = LogFactory.getLog(TokenizerMapperV1.class);
			log.info("Mylog@Mapper: " + word.toString());
		}
	}

	/**
	 * This method is used to check if the parameter string that is passed
	 * contains only letters in its content
	 * 
	 * @param term
	 *            - the string to check for characters
	 * @return - boolean value that indicates if the string is composed of
	 *         characters
	 */

	public boolean isCharacter(String term) {
		String expression = "^[a-zA-Z]*$";
		Pattern pattern = Pattern.compile(expression);
		Matcher matcher = pattern.matcher(term);

		return matcher.matches();

	}
}

/**
 * Class - Reducer class
 */
class DoubleSumReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();

	/**
	 * This method will process the data from the mapper to generate the final
	 * output in the reducer phase
	 * 
	 * @param key
	 *            - the first character of a tokenized word
	 * @param values
	 *            - the value list which contains the word length of the
	 *            corresponding key
	 * @param context
	 *            - allows the Mapper/Reducer to interact with the rest of the
	 *            Hadoop system
	 */
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int word_length_sum = 0;
		int word_count = 0;

		/*
		 * Iterates through the values array and generate a values of the total
		 * of the word length and the number of words in the values list
		 */
		for (IntWritable val : values) {
			word_length_sum += val.get();
			word_count++;
		}

		// Calculate the word average length
		result.set((word_length_sum / (word_count * 1.0)));
		context.write(key, result);

		// Reducer logging
		Log log = LogFactory.getLog(DoubleSumReducer.class);
		log.info("Mylog@Reducer: " + key.toString() + " " + result.toString());
	}
}

public class WordAvgLen1 {
	/**
	 * The main driver for average word count map/reduce program. Invoke this
	 * method to submit the map/reduce job.
	 * 
	 * @throws Exception
	 * @param args
	 *            - input and output file paths
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word avg1");

		job.setJarByClass(WordAvgLen1.class);
		job.setMapperClass(TokenizerMapperV1.class);
		job.setReducerClass(DoubleSumReducer.class);

		// the keys are the first characters of the tokenized words (strings)
		job.setMapOutputKeyClass(Text.class);
		// the values are counts (ints)
		job.setMapOutputValueClass(IntWritable.class);

		// the keys are the first characters of the tokenized words (strings)
		job.setOutputKeyClass(Text.class);
		// the output value of the reducer is the word average length of double
		// precision
		job.setOutputValueClass(DoubleWritable.class);

		// set the input and output file paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Exit on completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
