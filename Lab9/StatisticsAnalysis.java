import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StatisticsAnalysis {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "StatisticsAnalysis");

		job.setJarByClass(StatisticsAnalysis.class);

		job.setMapperClass(StatisticsMapper.class);
		job.setCombinerClass(StatisticsReducer.class);
		job.setReducerClass(StatisticsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path inputPath = new Path("hdfs://hadoop-master:9000/user/ict1902673/ufo/input/");
		Path outputPath = new Path("hdfs://hadoop-master:9000/user/ict1902673/ufo/output/" + new Date().getTime());// use
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit((job.waitForCompletion(true)) ? 0 : 1);
	}
}