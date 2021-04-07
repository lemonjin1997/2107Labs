import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StatisticsAnalysis {
//	public static void main(String[] args)throws Exception{
//		Configuration conf = new Configuration();
//		Job job = Job.getInstance(conf, "StatisticsAnalysis");
//		
//		job.setJarByClass(StatisticsAnalysis.class);
//		
//		job.setMapperClass(StatisticsMapper.class); 
//		job.setCombinerClass(StatisticsReducer.class); 
//		job.setReducerClass(StatisticsReducer.class);
//		
//		job.setOutputKeyClass(Text.class); 
//		job.setOutputValueClass(IntWritable.class);
//		Path inputPath = new Path("hdfs://hadoop-master:9000/user/ict1902654/ufo/input/");
//		Path outputPath = new Path("hdfs://hadoop-master:9000/user/ict1902654/ufo/onput/" 
//				+ new Date().getTime());//use run-time as output folder
//		
//		FileInputFormat.addInputPath(job, inputPath);
//		FileOutputFormat.setOutputPath(job, outputPath);
//		
//		System.exit((job.waitForCompletion(true))?0:1);
//	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] InputArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if (InputArgs.length < 1){
			System.err.println("Error: please provide one paths"); 
            System.exit(2);
		}
		
		Job job = Job.getInstance(conf,"SortAnalysis");
		job.setJarByClass(StatisticsAnalysis.class);
		
		job.setMapperClass(StatisticsMapper.class);
		
		job.setReducerClass(StatisticsReducer.class);
		
		job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
//		Path inputPath = new Path("hdfs://hadoop-master:9000/user/ict1902654/ufo/input/");
//		Path outputPath = new Path("hdfs://hadoop-master:9000/user/ict1902654/ufo/output/"
//						+ new Date().getTime());//run-time as output folder
		FileInputFormat.addInputPath(job,new Path(InputArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/user/phamvanvung/sortufo/output/"
				+ new Date().getTime()));
		
		System.exit(job.waitForCompletion(true)?0:1);

	}

}