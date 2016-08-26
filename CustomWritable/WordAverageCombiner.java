/**
 * 
 */
package comp9313.ass1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author comp9313
 *
 */
public class WordAverageCombiner extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {

	public void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		int word_length_sum = 0;
		int word_count = 0;

		for (IntArrayWritable val : values) {
			IntWritable[] value_array = val.get();
			word_length_sum += value_array[0].get();
			word_count += value_array[1].get();
		}
		IntWritable[] combined_value_array = new IntWritable[] { new IntWritable(word_length_sum),
				new IntWritable(word_count) };
		context.write(key, new IntArrayWritable(combined_value_array));
	}

}
