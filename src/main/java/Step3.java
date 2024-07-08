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


public class Step3 {

    //----------------------------------MAPPER--------------------------------------//

    private static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splittedKeyValue = value.toString().split("\t");
            String k = splittedKeyValue[0];
            String v = splittedKeyValue[1];
            
            String[] splittedKey = k.split(" ");
            String[] splittedValue = v.split(" ");
            String decade = splittedKey[0];
            String w1 = splittedKey[1];
            String w2 = splittedKey[2];

            if(w1.equals("*")){
                //<decade * *,occurences>
                if(w2.equals("*")){
                    context.write(new Text(k), new Text(v));
                }
                //<decade * w2,occurences>
                else{
                    String c_w2 = splittedValue[0];
                    context.write(new Text(String.format("%s %s 1", decade, w2)), new Text(c_w2));
                }
            }
            //<decade w1 w2,c(w1) c(w1w2)>
            else{
                String c_w1 = splittedValue[0];
                String c_w1w2 = splittedValue[1];
                context.write(new Text(String.format("%s %s 2", decade, w2)),new Text(String.format("%s %s %s", w1, c_w1, c_w1w2)));
            }
        }
    }


    //----------------------------------REDUCER--------------------------------------//

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        
        long N = 0;
        long c_w1 = 0;
        long  c_w2 = 0;
        long c_w1w2 = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            String[] splittedKey = key.toString().split(" ");
            String decade = splittedKey[0];
            String first = splittedKey[1];
            String second = splittedKey[2];

            if(first.equals("*")){
                for(Text value : values){
                    N = Long.parseLong(value.toString());
                }
            }
            
            else if(second.equals("1")){
                for(Text value : values){
                    c_w2 = Long.parseLong(value.toString());
                }
            } 

            else {
                for(Text value : values){
                    String[] splittedValue = value.toString().split(" ");
                    String w1 = splittedValue[0];
                    c_w1 = Long.parseLong(splittedValue[1]);
                    c_w1w2 = Long.parseLong(splittedValue[2]);
                    String npmi = String.valueOf(calculate_npmi(c_w1, c_w2, c_w1w2, N));
                    if(!npmi.equals("0.0")){
                        context.write(new Text(String.format("%s %s %s", decade, w1 , first)), new Text(npmi));
                    }
                }
            }
        }

        public static double calculate_npmi(long c_w1, long c_w2, long c_w1w2, long N){
            double numerator = Math.log((double) c_w1w2 / N) - Math.log((double) c_w1 / N) - Math.log((double) c_w2 / N);
            double denominator = -Math.log((double) c_w1w2 / N);
    
            if (Double.isNaN(numerator) || Double.isNaN(denominator) || denominator == 0) {
                return 0.0; // Handle division by zero or NaN
            }
            return numerator / denominator;
        }

    }

    //----------------------------------PARTITIONER--------------------------------------//

    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        // Every decade will come to the same partition
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs((key.toString().split(" ")[0]).hashCode() % numPartitions);
        }
    }

    //----------------------------------MAIN--------------------------------------//
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://bucket15032000/outputStep2"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket15032000/outputStep3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}