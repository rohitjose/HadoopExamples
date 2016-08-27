package comp9313.ass1;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

/**
 * Class - IntArrayWritable Data structure to hold the [<word length>,<word
 * count>] values to be emitted from the Mapper to find the mean word length.
 */

class IntArrayWritable extends ArrayWritable {

	/**
	 * Parameterized Constructor used to initialize the values of object
	 * 
	 * @param values
	 *            - Array of IntWritable This parameter will contain the values
	 *            [<word length>,<word count>] that will be propagated from the
	 *            Mapper class
	 */
	public IntArrayWritable(IntWritable[] values) {
		super(IntWritable.class, values);
	}

	/**
	 * Default constructor
	 */
	public IntArrayWritable() {
		super(IntWritable.class);
	}

	/**
	 * get() - The method returns the values [<word length>,<word count>]
	 * 
	 * @return Returns the IntWritable array of size two that contains both the
	 *         word length value and the count
	 */
	@Override
	public IntWritable[] get() {
		Writable[] values = super.get();

		IntWritable value1 = (IntWritable) values[0];
		IntWritable value2 = (IntWritable) values[1];

		return new IntWritable[] { value1, value2 };
	}

	/**
	 * toString() - Returns the Integer value in the [<word length>,<word
	 * count>] array as their respective string values
	 * 
	 * @return Returns the values of the object converted into string
	 */
	@Override
	public String toString() {
		IntWritable[] values = get();
		return values[0].toString() + ", " + values[1].toString();
	}

}

/**
 * Class - Mapper Class
 */
class TokenizerMapper extends Mapper<Object, Text, Text, IntArrayWritable> {

	private Text character_key = new Text(); // Stores the character key,the
												// first character of a word
	private final static IntWritable one = new IntWritable(1);

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

			if (isCharacter(word_character)) {// check for
												// alphabets
				character_key.set(word_character);
				// build the IntWritable array - [<word length>,<word count>]
				IntWritable[] value_array = new IntWritable[] { new IntWritable(current_word.length()), one };
				IntArrayWritable values = new IntArrayWritable(value_array);

				context.write(character_key, values);
			}
			// Mapper logging
			Log log = LogFactory.getLog(TokenizerMapper.class);
			log.info("Mylog@Mapper: " + character_key.toString());
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
 * Class - Combiner class
 */

class WordAverageCombiner extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {

	/**
	 * This method combines the multiple value pairs from a mapper to generate a
	 * more condensed list for the reducer. The method takes in the (<key>,
	 * <values[]>) emitted from the mapper and adds these values for a mapper.
	 * 
	 * @param key
	 *            - the first character of a tokenized word
	 * @param values
	 *            - the value pairs of the corresponding key
	 * @param context
	 *            - allows the Mapper/Reducer to interact with the rest of the
	 *            Hadoop system
	 */
	public void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		int word_length_sum = 0;
		int word_count = 0;

		/*
		 * Iterates through the values array and generate a value pair that
		 * includes the total of the word length and the number of words in the
		 * values list
		 */
		for (IntArrayWritable val : values) {
			IntWritable[] value_array = val.get();
			word_length_sum += value_array[0].get();
			word_count += value_array[1].get();
		}
		// Build the IntWritable[] array from the sum of word lengths and count
		IntWritable[] combined_value_array = new IntWritable[] { new IntWritable(word_length_sum),
				new IntWritable(word_count) };
		context.write(key, new IntArrayWritable(combined_value_array));

		// Combiner logging
		Log log = LogFactory.getLog(WordAverageCombiner.class);
		log.info("Mylog@Combiner: " + key.toString());
	}

}

/**
 * Class - Reducer class
 */
class WordAverageReducer extends Reducer<Text, IntArrayWritable, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();

	/**
	 * This method will process the data from the mapper to generate the final
	 * output in the reducer phase
	 * 
	 * @param key
	 *            - the first character of a tokenized word
	 * @param values
	 *            - the value pairs of the corresponding key
	 * @param context
	 *            - allows the Mapper/Reducer to interact with the rest of the
	 *            Hadoop system
	 */
	public void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		int word_length_sum = 0;
		int word_count = 0;

		/*
		 * Iterates through the values array and generate a values of the total
		 * of the word length and the number of words in the values list
		 */
		for (IntArrayWritable val : values) {
			IntWritable[] value_array = val.get();
			word_length_sum += value_array[0].get();
			word_count += value_array[1].get();
		}

		// Calculate the word average length
		result.set((word_length_sum / (word_count * 1.0)));
		context.write(key, result);

		// Reducer logging
		Log log = LogFactory.getLog(WordAverageReducer.class);
		log.info("Mylog@Reducer: " + key.toString() + " " + result.toString());

	}
}

public class WordAvgLen2 {

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
		Job job = Job.getInstance(conf, "word avg2");

		job.setJarByClass(WordAvgLen2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(WordAverageCombiner.class);
		job.setReducerClass(WordAverageReducer.class);

		// the keys are the first characters of the tokenized words (strings)
		job.setMapOutputKeyClass(Text.class);
		// the values are counts within the IntArrayWritable class
		job.setMapOutputValueClass(IntArrayWritable.class);

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
