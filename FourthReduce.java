import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FourthReduce extends Reducer<Text, Text, Text, Text> {
	
	
	/** Reducer setup */
	public void setup (Context context) {

	}

	
    //@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	//Reduce:
    	//		Input (author, [word=m])
    	//		Output (author, word=m)
    	
    	for (Text val : values){
    		String[] value = val.toString().split("=");
    		String word = value[0];
    		String tdif = value[1];
    		context.write(new Text(key.toString()), new Text(word + "=" + tdif));
    		
    	}
    	
    }
    
}