import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class Step2 {

    //----------------------------------MAPPER--------------------------------------//

    private static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] splits = value.toString().split("\t");
            String k = splits[0];
            String v = splits[1];
            String[] splittedKey = k.split(" ");

            String decade = splittedKey[0];
            String w1 = splittedKey[1];
            String w2 = splittedKey[2];
            String occurences = v;
            
            //<decade * *,occurences> or <decade * w2,occurences>
            if(w1.equals("*")){
                context.write(new Text(k), new Text(v));
            }

            else{
                //<decade w1 *,occurences>
                if(w2.equals("*")){
                    context.write(new Text(String.format("%s %s 1", decade, w1)), new Text(occurences));
                }
                //<decade w1 w2,occurences>
                else{
                    context.write(new Text(String.format("%s %s 2", decade, w1)), new Text(String.format("%s %s", w2, occurences)));
                }
            }
    }
}


    //----------------------------------REDUCER--------------------------------------//

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
       
        String c_w1 = "";

        @Override
        protected void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            
            String[] splittedKey = key.toString().split(" ");
            String decade = splittedKey[0];
            String first = splittedKey[1];
            String second = splittedKey[2];

            if(first.equals("*")){
                for(Text value : Values){
                    context.write(key, value);
                }
            }

            else{
                if(second.equals("1")){
                    for(Text value : Values){
                        c_w1 = value.toString();
                    }
                }
                else{
                    for(Text value : Values){
                        String[] splittedValue = value.toString().split(" ");
                        String w2 = splittedValue[0];
                        String c_w1w2 = splittedValue[1];
                        context.write(new Text(String.format("%s %s %s", decade, first, w2)), new Text(String.format("%s %s", c_w1, c_w1w2)));
                    }    
                }
            }
        }
    }


    //----------------------------------PARTITIONER--------------------------------------//

    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs((key.toString().split(" ")[0]).hashCode() % numPartitions);
        }
    }

    
    // ====================== MAIN ====================== //
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://bucket15032000/outputStep1"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket15032000/outputStep2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

