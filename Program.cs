using Microsoft.Spark.Sql;
using System.IO;
using static Microsoft.Spark.Sql.Functions;

namespace MySparkApp
{
    /***
     * Command to run the program from local:
     * spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local C:\Users\absibal\source\repos\SparkTest\SparkTestPublish\microsoft-spark-2-4_2.11-1.0.0.jar dotnet C:\Users\absibal\source\repos\SparkTest\SparkTestPublish\SparkTest.dll C:\Users\absibal\Downloads\SampleDoc.txt
    ***/
    partial class Program
    {
        static void Main(string[] args)
        {
            // Create Spark session
            SparkSession spark =
                SparkSession
                    .Builder()
                    .AppName("word_count_sample")
                    .GetOrCreate();

            // Create initial DataFrame
            string filePath = args[0];
            DataFrame dataFrame = spark.Read().Text(filePath);

            //Count words
            DataFrame words =
                dataFrame
                    .Select(Split(Col("value"), " ").Alias("words"))
                    .Select(Explode(Col("words")).Alias("word"));

            DataFrame wordCount = words
                    .GroupBy("word")
                    .Count()
                    .OrderBy(Col("count").Desc());

            // -------------------------------

            var getWordLength = Udf<string, int>(w => w.Length);

            DataFrame wordLength = words
                .Select(words["word"], getWordLength(words["word"]).Alias("WordLength"))
                .Distinct()
                .OrderBy(Col("WordLength").Desc());

            // -------------------------------

            // Display results

            wordCount.WriteAsCsvWithHeader(Path.Join(args[1], "wordCount"));
            wordLength.WriteAsCsvWithHeader(Path.Join(args[1], "wordLength"));

            // Stop Spark session
            spark.Stop();
        }
    }
}