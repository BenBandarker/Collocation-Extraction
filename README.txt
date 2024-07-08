DSP Course

Assignment 2: Collocation Extraction

Authors:
    Shira Edri 211722764
    Ben Bandarkar 318468758

Run instructions:

    1. start the lab and update credentials.
    2. create a bucket X is S3 and update the path in all the relevents locations in the code.
    3. make a jar file for :Step1.java, Step2.java, Step3.java, Step4.java.
    4. upload the jar files to bucket X.
    5. make sure pom.xml content compatible to our version.
    6. compile(jar) ExtractCollocations.java.
    7. run the next command in terminal: java -jar *path_to_ExtractCollocation_jar* minPmi relMinPmi
    8. output will wait in the output path that defines in the main function of step4.
    9. make sure to monitor the lab time, in some cases the program can take more than 4 hours.


# Step 1 

Mapper:
Input:
Key = lineId (LongWritable)
Value = 2-gram \t year \t occurrences \t...
Output:
1) <decade w1 w2,count(w1w2)>
2) <decade * *,N>
3) <decade w1 *,count(w1)>
4) <decade * w2,count(w2)>

Reducer:
Output:
for each key, merge the value.


# Step 2 

Mapper:
Input:
1) Key = lineId ,value = <decade w1 w2 \t count(w1w2)>
2) Key = lineId ,value = <decade * * \t N>
3) Key = lineId ,value = <decade w1 * \t count(w1)>
4) Key = lineId ,value = <decade * w2 \t count(w2)>
Output:
1) <decade w1 2,w2 count(w1w2)>
2) <decade * *,N>
3) <decade w1 1,count(w1)>
4) <decade * w2,count(w2)>

Reducer:
Input:
Output:
1) <decade w1 w2, c(w1) count(w1w2)>
2) <decade * *,N>
3) <decade * w2,count(w2)>


# Step 3 

The purpose of step 3 is to unite all the parameters required to calculate npmi value.

Mapper:
Input:
1) key = lineId, value = <decade w1 w2 \t c(w1) count(w1w2)>
2) key = lineId, value = <decade * * \t N>
3) key = lineId, value = <decade * w2 \t count(w2)>
Output:
2) <decade * *,N>
3) <decade w2 2, w1 count(w1) count(w1w2)>
4) <decade w2 1, count(w2)>

Reducer:
Output:
1) <decade w1 w2, npmi>


# Step 4

Mapper:
Input:
1) key = lineId, value = <decade w1 w2, npmi>
Output:
1)<decade w1 w2 npmi, "">
2)<decade * * *, npmi>

Reducer:
Output:
1)<decade w1 w2 npmi, "">

