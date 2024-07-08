import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;



public class Step1 {

    //----------------------------------MAPPER--------------------------------------//

    private static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

            public static HashSet<String> EnglishStopWords = new HashSet<>(Arrays.asList("a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost",
            "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst",
            "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are",
            "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before",
            "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both",
            "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry",
            "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven",
            "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere",
            "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly",
            "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt",
            "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him",
            "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is",
            "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me",
            "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself",
            "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor",
            "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other",
            "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please",
            "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several",
            "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow",
            "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system",
            "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there",
            "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
            "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to",
            "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under",
            "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever",
            "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein",
            "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole",
            "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your",
            "yours", "yourself", "yourselves"));


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] fields = value.toString().split("\t");
            String[] bigram = fields[0].split(" ");
            String year = fields[1];
            String decade = String.valueOf((Integer.parseInt(year) / 10) * 10);

            if (bigram.length == 2 && fields.length > 2) {
                String w1 = bigram[0].substring(1);
                String w2 = bigram[1];
                double chance = Math.random();
                if(chance < 0.5){ //half corpus
                    if (EnglishStopWords.contains(w1) || EnglishStopWords.contains(w2))
                        return;
                    if(!isValidWord(w1) || !isValidWord(w2))
                        return;
                    
                    Text occurrences = new Text(fields[2]);
                    
                    context.write(new Text(String.format("%s * *", decade)), occurrences);
                    context.write(new Text(String.format("%s %s *", decade, w1)), occurrences);
                    context.write(new Text(String.format("%s * %s", decade, w2)), occurrences);
                    context.write(new Text(String.format("%s %s %s", decade, w1, w2)), occurrences); 
                } 
            }
        }

        private static boolean isValidWord(String word) {
            return word != null && word.matches("[a-zA-Z]+") && word.length() > 1;
        }

    }


    //----------------------------------REDUCER--------------------------------------//

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            long sumPerKey = 0;
            for (Text value : Values) {
                long longVal = Long.parseLong(value.toString());
                sumPerKey += longVal; 
            }
            context.write(key, new Text(String.valueOf(sumPerKey)));
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
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        //long MaxSplitSize = 16000000;
        //conf.setLong("mapreduce.input.fileinputformat.split.maxsize", MaxSplitSize);
        Job job = Job.getInstance(conf, "Step 1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class); //optional
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket15032000/outputStep1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
