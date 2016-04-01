

	import java.io.IOException;
	import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.lib.input.FileSplit;


	public class FirstMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		
		/** Map setup */
		public void setup (Context context) {
			
		}

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	    	//Map:
	    	//	Input (LineNr, Line in document)
	    	//	Split the line into words and output each word:
	    	//	Output (word;filename, 1)
	    	
	    	
	    	String delims = "<===>";
	    	String text = value.toString();
	 
	    	StringTokenizer st = new StringTokenizer(text, delims);
	    	
	    	String[] tokens = text.split(delims);
	    	String author = tokens[0];
	    	tokens[1] = tokens[1].toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", "");
	    	String[] sentence = tokens[1].split(" ");
	    	
	    	for(int j = 0; j < sentence.length; j++){
	    		Text currentWord = new Text();
		    	for(String word : WORD_BOUNDARY.split(sentence[j])){
		    		currentWord = new Text(word.trim() + "@" + author);

					context.write(currentWord, one);
		    	}
	    	}
	    	    

	    }
	}

