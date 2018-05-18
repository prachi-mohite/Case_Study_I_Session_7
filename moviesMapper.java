
import java.io.IOException;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class moviesMapper extends
		Mapper<LongWritable, Text, Text, Text> {
			
		@Override
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
			try {
				if (key.get() == 0 && value.toString().contains("movieId")) //Ignoring Line 1, which has columns' names
				{ 
				    return;
				} 
				else 
				{
					String record = value.toString(); //Reading Record
					String[] parts = record.split(","); 
					context.write(new Text(parts[0]), new Text("movies\t" + parts[1])); //Part 0 has movie id - Part 1 - has Movie Name
				}
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
	}
}