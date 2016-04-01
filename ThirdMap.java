
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdMap extends Mapper<LongWritable, Text, Text, Text> {
	

	/** Map setup */
	public void setup (Context context) {
		
	}

	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	//Map:
    	//	Input (word;author, n;N)
    	//	Move author to value field and add 1 to the end of value field
    	//	Output (word, author;n;N;1)
    	
    	String[] wordAndCounters = value.toString().split("\t");
        String[] wordAndDoc = wordAndCounters[0].split("@");                 //3/1500
        context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
    	
    }
}
