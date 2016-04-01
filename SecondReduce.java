import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReduce extends Reducer<Text, Text, Text, Text> {
	
	
	/** Reducer setup */
	public void setup (Context context) {

	}

	
    //@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	//Reduce:
    	//		Input (author, [word;n])
    	//		Output (word;author, n;N)
    	
    	context.getCounter(COUNTERS.AUTHORS).increment(1);
    	
    	int sumPerAuthor = 0;
    	Map<String, Integer> tempCounter = new HashMap<String, Integer>();
    	
    	for (Text val : values) {
    		String[] wordCounter = val.toString().split("=");
            tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
            sumPerAuthor += Integer.parseInt(val.toString().split("=")[1]);
    	}
    	for (String wordKey : tempCounter.keySet()) {
            context.write(new Text(wordKey + "@" + key.toString()), new Text(tempCounter.get(wordKey) + "/"
                    + sumPerAuthor));
        }
    	
    }
    
}
