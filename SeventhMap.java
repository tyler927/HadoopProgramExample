import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SeventhMap extends Mapper<LongWritable, Text, Text, Text> {
	

	/** Map setup */
	public void setup(Context context) {

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//Map:
    	//	Input (word author, n)
    	//	change the key to be only author, and move the word into value
    	//	Output (author, word;n)
		
		String[] wordAndAuthCounter = value.toString().split("\t");
        String[] wordAndAuth = wordAndAuthCounter[1].split("=");
        context.write(new Text(wordAndAuthCounter[0]), new Text(wordAndAuth[0] + "=" + wordAndAuth[1]));

	}
}
