import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	
	/** Reducer setup */
	public void setup (Context context) {

	}

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	
    	//Reduce:
    	//	Input (word;filename, [counts])
    	//	Sum all the counts as n
    	//	Output (word;filename, n)
    	
    	int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
    }
    
}