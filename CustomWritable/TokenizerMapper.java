package comp9313.ass1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntArrayWritable> {

	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
		while (itr.hasMoreTokens()) {
			String current_word = itr.nextToken().toLowerCase();
			String word_character = String.valueOf(current_word.charAt(0));

			if (Character.isLetter(word_character.charAt(0))) {
				word.set(word_character);
				IntWritable[] value_array = new IntWritable[] { new IntWritable(current_word.length()), one };
				IntArrayWritable values = new IntArrayWritable(value_array);

				context.write(word, values);
			}

			Log log = LogFactory.getLog(TokenizerMapper.class);
			log.info("Mylog@Mapper: " + word.toString());
		}
	}
}