import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SixthReduce extends Reducer<Text, Text, Text, Text> {
	
	private static final DecimalFormat DF = new DecimalFormat("###.#######");
	/** Reducer setup */
	public void setup (Context context) {

	}

	
    //@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	//Reduce:
    	//		Input (author, [word;n])
    	//		Output (word;author, n;N)
    	
    	
    	int sumPerAuthor = 0;
    	Map<String, Integer> tempCounter = new HashMap<String, Integer>();
    	
    	for (Text val : values) {
    		String[] wordCounter = val.toString().split("=");
            tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
            sumPerAuthor += Integer.parseInt(val.toString().split("=")[1]);
    	}
    	for (String wordKey : tempCounter.keySet()) {
    		double tf = (Double.valueOf(tempCounter.get(wordKey)))/sumPerAuthor;
            context.write(new Text(key.toString()), new Text(wordKey + "=" + DF.format(tf)));
        }
    	
    }
    
}
