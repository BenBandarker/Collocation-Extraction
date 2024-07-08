import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Step4 {

    //----------------------------------MAPPER--------------------------------------//

    private static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //<decade w1 w2,npmi>
            String[] splittedValue = value.toString().split("\t");
            String k = splittedValue[0];
            String v = splittedValue[1];
            
            String[] splittedkey = k.split(" ");
            String decade = splittedkey[0];
            String w1 = splittedkey[1];
            String w2 = splittedkey[2];
            
            context.write(new Text(String.format("%s %s %s %s", decade, w1, w2, v)), new Text(""));
            context.write(new Text(String.format("%s * * *", decade)), new Text(v));
        }

    }

    //----------------------------------COMPARISON--------------------------------------//
    
    private static class Comparison extends WritableComparator {
        protected Comparison() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) { 
            String[] key1 = ((Text) a).toString().split(" ");
            String[] key2 = ((Text) b).toString().split(" ");

            String decade1 = key1[0];
            String decade2 = key2[0];
            String w11 = key1[1];
            String w12 = key1[2];
            String w21 = key2[1];
            String w22 = key2[2];
            Double npmi1 = Double.parseDouble(key1[3]);
            Double npmi2 = Double.parseDouble(key2[3]);

            int decadeComparison = Integer.compare(Integer.parseInt(decade1), Integer.parseInt(decade2));
            if (decadeComparison != 0) {
                return decadeComparison;
            }

            int npmiComparison = Double.compare(npmi2, npmi1); // Descending order
            if (npmiComparison != 0) {
                return npmiComparison;
            }

            int wordComparison = w11.compareTo(w21);
            if (wordComparison != 0) {
                return wordComparison;
            }
            
            return w12.compareTo(w22);
        }
    }

    //----------------------------------REDUCER--------------------------------------//

    private static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private double sumNpmiPerDecade = 0.0;
        private static double minPmi;
        private static double relMinPmi;

        public void setup(Context context) throws IOException, InterruptedException {
            minPmi = context.getConfiguration().getDouble("minPmi" , -1);
            relMinPmi = context.getConfiguration().getDouble("relMinPmi" , -1);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            
            String[] splittedKey = key.toString().split(" ");
            String decade = splittedKey[0];
            String first = splittedKey[1];
            String second = splittedKey[2];
            String third = splittedKey[3];
            
            if(first.equals("*")){
                Double sumPerKey = 0.0;
                for(Text value : Values){
                    sumPerKey += Double.parseDouble(value.toString());
                }
                sumNpmiPerDecade = sumPerKey;
            }

            else{
                for(Text value : Values){
                    double npmi = Double.parseDouble(third.toString());
                    if(isCollocation(npmi, sumNpmiPerDecade)){
                        context.write(new Text(String.format("%s %s %s %s", decade, first, second, npmi)), new Text(""));
                    }
                }
            }
        }

        public static boolean isCollocation(Double npmi, Double sumNpmiPerDecade){
            return (npmi > minPmi ||  (npmi / sumNpmiPerDecade) > relMinPmi) && (npmi < 1);
        }

    }

    //----------------------------------PARTITIONER--------------------------------------//


    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String [] split_key = key.toString().split(" ");
            String decade = split_key[0]+ "ShiraEdri"; //partition by decade, "ShiraEdri" added for better distribution of the keys by decade
            return (decade.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    
    //----------------------------------MAIN--------------------------------------//
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        conf.setDouble("minPmi", Double.parseDouble(args[1]));
        conf.setDouble("relMinPmi", Double.parseDouble(args[2]));
        Job job = Job.getInstance(conf, "Step 4");
        job.setJarByClass(Step4.class);
        job.setSortComparatorClass(Step4.Comparison.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://bucket15032000/outputStep3"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket15032000/outputStep4"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
