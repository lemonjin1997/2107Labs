import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

//public class StatisticsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
//	IntWritable totalIW = new IntWritable();
//	
//	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)throws IOException, InterruptedException{
//		int total = 0;
//		for(IntWritable value:values){
//			total += value.get();
//		}
//		totalIW.set(total);
//		context.write(key, totalIW);
//	}
//
//}

public class StatisticsReducer extends Reducer<Text,LongWritable,LongWritable,Text>{
	
	private TreeMap<Long,String> tmap2;
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException
	{
		tmap2 = new TreeMap<Long,String>();
	}
	
	@Override
	public void reduce(Text key, Iterable<LongWritable> values,
			Context context) 
					throws IOException,InterruptedException
					{
						String name = key.toString();
						long total = 0;
						
						for (LongWritable val : values){
							total = val.get();
						}
						
						tmap2.put(total,name);
						
						if(tmap2.size() > 5){
							tmap2.remove(tmap2.firstKey());
						}
		
					}
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException{
		for(Entry<Long, String> entry : tmap2.entrySet())
		{
			String name = entry.getValue();
			long total = entry.getKey();
			context.write(new LongWritable(total), new Text(name));
			
		}
	}

}