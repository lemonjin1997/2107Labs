import java.io.IOException;
import java.util.TreeMap;
import java.io.*;
import java.util.*;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

//public class StatisticsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
//	Text shape = new Text();
//	IntWritable one = new IntWritable(1);
//
//	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)throws IOException, InterruptedException{
//		String[] parts = value.toString().split("\t");
//		String shapeStr;
//		if(parts.length == 6 && parts[3]!=null){
//			shapeStr = parts[3].trim();
//			if(shapeStr!=null && !shapeStr.isEmpty()){
//				shape.set(shapeStr);
//				context.write(shape,one);
//			}
//		}
//	}
//}

public class StatisticsMapper extends Mapper<Object,Text,Text,LongWritable> {
	
	private TreeMap<Long,String> tmap;
	
	@Override
    public void setup(Context context) throws IOException,InterruptedException
	{
		tmap = new TreeMap<Long,String>(); 
    }
	
	@Override
	protected void map(Object key,Text value,
			Context context)throws IOException,InterruptedException{
		
		String[] tokens = value.toString().split("\t");
		String shapes = tokens[0];
		long total = Long.parseLong(tokens[1]);
		
		tmap.put(total,shapes);
		
		if(tmap.size() > 5){
			tmap.remove(tmap.firstKey());
		}
		
	}
	
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException{
		for(Map.Entry<Long,String> entry : tmap.entrySet()){
			String Shapes = entry.getValue();
			long totals = entry.getKey();
			
			context.write(new Text(Shapes),new LongWritable(totals));
		}
	}
	
}