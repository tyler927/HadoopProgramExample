
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Controller {

	
	static Configuration conf;
	Long NumAuthors;
	
    public static void main(String[] args) throws Exception {
    	
    	Controller skel = new Controller();
    	
    	conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

    	String pathin = args[0];
    	String pathinUnkown = args[1];
    	String pathout = args[2];
    	

    	// Run first Mapreduce job
    	fs.delete(new Path(pathout+"_1"), true);
    	skel.runFirstJob(pathin, pathout+"_1");
    	
    	// count authors in the second reduce
    	
    	// Run second Mapreduce job
    	fs.delete(new Path(pathout+"_2"), true);
    	skel.runSecondJob(pathout+"_1", pathout+"_2");
    	
    	// Run third Mapreduce job
    	fs.delete(new Path(pathout+"_3"), true);
    	skel.runThirdJob(pathout+"_2", pathout+"_3");
    	
   	// Run fourth Mapreduce job
    	fs.delete(new Path(pathout+"_4"), true);
		skel.runFourthJob(pathout+"_3", pathout+"_4");
		
    	// Run fifth Mapreduce job
    	fs.delete(new Path(pathout+"_5"), true);
		skel.runFifthJob(pathinUnkown, pathout+"_5");
    	
		
		// Run sixth Mapreduce job
    	fs.delete(new Path(pathout+"_6"), true);
		skel.runSixthJob(pathout+"_5", pathout+"_6");
		
		// Run seventh Mapreduce job
    	fs.delete(new Path(pathout+"_7"), true);
		skel.runSeventhJob(pathout+"_4", pathout+"_6", pathout+"_7");
    	
    	
    }

    // Word count
    public int runFirstJob(String pathin, String pathout) throws Exception {
		
    	Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "first");
		
		job.setJarByClass(Controller.class);

		job.setMapperClass(FirstMap.class);
		job.setReducerClass(FirstReduce.class);
        
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);	
		
		FileInputFormat.addInputPath(job, new Path(pathin));
		FileOutputFormat.setOutputPath(job, new Path(pathout));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : -1;
    }
    
    // Term frequency
	public int runSecondJob(String pathin, String pathout) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "secound");
		
		job.setJarByClass(Controller.class);

		job.setMapperClass(SecondMap.class);
		job.setReducerClass(SecondReduce.class);
        
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
		
		FileInputFormat.addInputPath(job, new Path(pathin));
		FileOutputFormat.setOutputPath(job, new Path(pathout));

		boolean success = job.waitForCompletion(true);
		
		long auth = job.getCounters().findCounter("COUNTERS", "AUTHORS").getValue();
		
		System.out.println("Authors = " + auth);
		NumAuthors = auth;

		
		return success ? 0 : -1;
    }

	// Tf-idf
	public int runThirdJob(String pathin, String pathout) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "third");
		
		
		
		job.setJarByClass(Controller.class);

		job.setMapperClass(ThirdMap.class);
		job.setReducerClass(ThirdReduce.class);
        
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
		
		FileInputFormat.addInputPath(job, new Path(pathin));
		FileOutputFormat.setOutputPath(job, new Path(pathout));
		
		job.setJobName(String.valueOf(NumAuthors));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : -1;
    }
	
	// AAVs
	public int runFourthJob(String pathin, String pathout) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "fourth");
		
		job.setJarByClass(Controller.class);

		job.setMapperClass(FourthMap.class);
		job.setReducerClass(FourthReduce.class);
        
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
		
		FileInputFormat.addInputPath(job, new Path(pathin));
		FileOutputFormat.setOutputPath(job, new Path(pathout));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : -1;
    }
	
	// Wordcount for unknown
	public int runFifthJob(String pathin, String pathout) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "fifth");

		job.setJarByClass(Controller.class);

		job.setMapperClass(FifthMap.class);
		job.setReducerClass(FifthReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(pathin));
		FileOutputFormat.setOutputPath(job, new Path(pathout));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : -1;
	}

	// Tf for unknown
	public int runSixthJob(String pathin, String pathout) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "sixth");

		job.setJarByClass(Controller.class);

		job.setMapperClass(SixthMap.class);
		job.setReducerClass(SixthReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(pathin));
		FileOutputFormat.setOutputPath(job, new Path(pathout));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : -1;
	}
	
	// Combine both files
	public int runSeventhJob(String pathin, String pathinunknown, String pathout) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "seventh");

		job.setJarByClass(Controller.class);

		job.setMapperClass(SeventhMap.class);
		job.setReducerClass(SeventhReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(pathin));
		FileInputFormat.addInputPath(job, new Path(pathinunknown));
		FileOutputFormat.setOutputPath(job, new Path(pathout));
		
		job.setJobName(String.valueOf(NumAuthors));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : -1;
	}
	
}