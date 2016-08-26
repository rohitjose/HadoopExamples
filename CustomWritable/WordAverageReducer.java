package comp9313.ass1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

public class WordAverageReducer extends Reducer<Text, IntArrayWritable, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();

	public void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		int word_length_sum = 0;
		int word_count = 0;

		for (IntArrayWritable val : values) {
			IntWritable[] value_array = val.get();
			word_length_sum += value_array[0].get();
			word_count += value_array[1].get();
		}

		result.set((word_length_sum / (word_count * 1.0)));
		context.write(key, result);

		Log log = LogFactory.getLog(WordAverageReducer.class);
		log.info("Mylog@Reducer: " + key.toString() + " " + result.toString());

		System.out.println(key.toString() + " " + result.toString());
	}
}