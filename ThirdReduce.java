
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.text.DecimalFormat;

public class ThirdReduce extends Reducer<Text, Text, Text, Text> {
	
	
	/** Reducer setup */
	public void setup (Context context) {

	}

	private static final DecimalFormat DF = new DecimalFormat("###.#######");
	
	Map<String, Integer> authorMax = new HashMap<String, Integer>();
	
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
    	//Reduce:
    	//	Input (word, [author;n;N;1])
    	//	Calculate the sum of value fiels last entries as m and move filename back to key
    	//	Again, you will have to look through values twice, once to find the sum and once to print out all the entries again. And you will have to store the values somewhere again.
    	//	Output (word;filename, n;N;m)
    	
   	Configuration conf = context.getConfiguration();
    	String param = conf.get("authors");
//    	System.out.println("++++++++++++++++++++++AUTHORS IN THRID" + param);
    	
    	// get the number of documents indirectly from the file-system (stored in the job name on purpose)
        int numberOfAuthorsTotal = Integer.parseInt(context.getJobName());
        // total frequency of this word
        int numberOfAuthorsKeyAppears = 0;
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        for (Text val : values) {
            String[] authorAndFrequencies = val.toString().split("=");
            numberOfAuthorsKeyAppears++;
            tempFrequencies.put(authorAndFrequencies[0], authorAndFrequencies[1]);
        }
        
        int maxcount = 0;
        for (String authorName : tempFrequencies.keySet()) {
            String[] wordFrequenceAndTotalWords = tempFrequencies.get(authorName).split("/");
            if(Integer.parseInt(wordFrequenceAndTotalWords[0]) > maxcount)
            	maxcount = Integer.parseInt(wordFrequenceAndTotalWords[0]);
        }
        
        
        
        for (String authorName : tempFrequencies.keySet()) {
//        	Double maxcount = 0.0;
        	String[] wordFrequenceAndTotalWords = tempFrequencies.get(authorName).split("/");
//        	
//        	for(String value : wordFrequenceAndTotalWords){
//        		if(maxcount < Double.valueOf(value))
//        			maxcount = Double.valueOf(value);
//        	}
        	
            
//            if(Integer.parseInt(wordFrequenceAndTotalWords[0]) > maxcount)
//            	maxcount = Integer.parseInt(wordFrequenceAndTotalWords[0]);
            //Term frequency is the quotient of the number of terms in document and the total number of terms in doc
            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
                    / maxcount);
 
            //inverse document frequency quotient between the number of docs in corpus and number of docs the term appears
            double idf = (double) numberOfAuthorsTotal / (double) numberOfAuthorsKeyAppears;
 
            //given that log(10) = 0, just consider the term frequency in documents
            double tfIdf = numberOfAuthorsTotal == numberOfAuthorsKeyAppears ?
                    0.0000000 : tf * (Math.log(idf) / Math.log(2));
 
//            context.write(new Text(key + "@" + authorName), new Text("TF: " + tf + "[" + numberOfDocumentsInCorpusWhereKeyAppears + "/"
//                    + numberOfDocumentsInCorpus + " , " + wordFrequenceAndTotalWords[0] + "/"
//                    + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]"));
            
            //context.write(new Text(key + "@" + authorName), new Text("TF: " + tf + " IDF: "
            		  // + idf + " TFIDF: " + DF.format(tfIdf) + "]"));
            
             context.write(new Text(key + "@" + authorName), new Text(DF.format(idf) + "&" + DF.format(tfIdf)));
        }

    }
    
}
