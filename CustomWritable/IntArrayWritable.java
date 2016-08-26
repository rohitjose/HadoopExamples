/**
 * Class - IntArrayWritable
 * Data structure to hold the [<word length>,<word count>] values
 * to be emitted from the Mapper to find the mean word length.
 */
package comp9313.ass1;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable {

	/**
	 * @param values
	 *            - Array of IntWritable This parameter will contain the values
	 *            [<word length>,<word count>] that will be propagated from the
	 *            Mapper class
	 */
	public IntArrayWritable(IntWritable[] values) {
		super(IntWritable.class, values);
	}

	public IntArrayWritable() {
		super(IntWritable.class);
	}

	/**
	 * get() - The function returns the values [<word length>,<word count>]
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
	 */
	@Override
	public String toString() {
		IntWritable[] values = get();
		return values[0].toString() + ", " + values[1].toString();
	}

}
