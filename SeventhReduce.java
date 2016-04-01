import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SeventhReduce extends Reducer<Text, Text, Text, Text> {

	private static final DecimalFormat DF = new DecimalFormat("###.#######");

	/** Reducer setup */
	public void setup(Context context) {

	}
	

	Map<String, Double> allWords = new HashMap<String, Double>();
	Map<String, Double> unknown = new HashMap<String, Double>();
	Map<String, Map<String, Double>> everything = new HashMap<String, Map<String, Double>>();
	
	TreeMap<Double, String> treemap = new TreeMap<Double, String>();
	int authorCount = 0;

	// @Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Reduce:
		// Input (author, [word;n])

		Configuration conf = context.getConfiguration();
		//System.out.println("++++++++++++++++++++++++++++++++++++++" + key.toString());
		int totalAuthors = Integer.parseInt(context.getJobName());

		if (authorCount < Integer.valueOf(totalAuthors)) {
			if (!key.toString().equals("Unknown")) {
				Map<String, Double> temp = new HashMap<String, Double>();
				for (Text val : values) {
					String[] wordAndNumbers = val.toString().split("=");
					String[] numbers = wordAndNumbers[1].split("&");
					if (!allWords.containsKey(wordAndNumbers[0])) {

						allWords.put(wordAndNumbers[0], Double.valueOf(numbers[0]));
					}
					
					temp.put(wordAndNumbers[0], Double.valueOf(numbers[0]));
					//System.out.println(temp);
					
				}
				everything.put(key.toString(), temp);
			} else {
				for (Text val : values) {
					String[] wordAndNumbers = val.toString().split("=");
					if (allWords.containsKey(wordAndNumbers[0])) {
						double tf = Double.valueOf(wordAndNumbers[1]);
						double idf = allWords.get(wordAndNumbers[0]);

						double tfIdf = tf * (Math.log(idf) / Math.log(2));

						unknown.put(wordAndNumbers[0], Double.valueOf(DF.format(tfIdf)));

						// context.write(new Text(key), new
						// Text(wordAndNumbers[0] + "=" + DF.format(tfIdf)));
					}
				}
			}
			authorCount++;
		}
		else if(authorCount == Integer.valueOf(totalAuthors)){

				
			for(String author : everything.keySet()){ // loops though all authors 
				//System.out.println("++++++++++++++++++++++++++++" + author);
				double top = 0;
				double a = 0;
				double b = 0;
				Map<String, Double> currentAuthor = everything.get(author); // gets current authors words and tdidf
				double cosine;

				for(String unknownWord : unknown.keySet()){ // loops though all words unknown author uses 
					
					if(currentAuthor.containsKey(unknownWord)){ // if current author uses word u 
						//System.out.println(unknownWord);
						top = top +  (Double.valueOf(currentAuthor.get(unknownWord)) * Double.valueOf(unknown.get(unknownWord)));
						a = a + Math.pow(Double.valueOf(currentAuthor.get(unknownWord)), 2);
						b = b + Math.pow(Double.valueOf(unknown.get(unknownWord)), 2);
					}
				}
				
				cosine = top / ((Math.sqrt(a)) * (Math.sqrt(b)));
//				System.out.println("Top: " + top);
//				System.out.println("A: " + a);
//				System.out.println("B: " + b);
				
				treemap.put(cosine, author);
				
				
				
				
				//context.write(new Text(author), new Text(DF.format(Double.valueOf(cosine))));
			}
			
			int ten = 10;
			int i = 0;
			for (Entry<Double, String> entry : treemap.entrySet()) {
			     if(i < ten){
			    	 //context.write(new Text(entry.getKey().toString()), new Text(entry.getValue().toString()));
			    	 context.write(new Text(entry.getValue().toString()), new Text(""));
			    	 i++;
			     }
			     else
			    	 break;
			     
			}
		}
		
		
		

	} // reduce 

} // SeventhReduce




















