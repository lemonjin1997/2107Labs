
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ANSValidationMapper extends Mapper<LongWritable, Text, Text, Text> {

	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, 
			Text, Text>.Context context) throws IOException, InterruptedException {
		int count = 0;
		if(isValid(value.toString())) {
			String[] parts = value.toString().split(",");
			
			
			if(!parts[10].isEmpty() && parts[14].equals("negative") && !parts[14].isEmpty() ){
				count++;
				System.out.println("validationMapper\t" + parts[10] + "\t" + parts[14] + count);
				context.write(new Text(parts[10]), new Text("countryCount\t"+Integer.toString(count)));
			}
		}


		
	}
	
	private boolean isValid(String line){
		String[] parts = line.split(",");
		if (parts.length == 27) {
			return true;
		} else {
			return false;
		}
	}


}