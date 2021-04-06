
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ANSReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
		int count = 0;
		String country = "";
		for (Text value: values) {
			
			String line[] = value.toString().split("\t");
			if(line[0].equals("country")){
				country = line[1];
			}
			else if(line[0].equals("countryCount")){
				
				count+= Integer.parseInt(line[1]);
			}
			
		}

		System.out.println("reducer\t"+country + "\t" + count);
		context.write(new Text(country), new Text(Integer.toString(count)));
	}

}