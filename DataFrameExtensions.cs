using Microsoft.Spark.Sql;
using System.IO;
using static Microsoft.Spark.Sql.Functions;

namespace MySparkApp
{
    /// <summary>
    /// Data frame extensions
    /// </summary>
    public static class DataFrameExtensions
    {
        /// <summary>
        /// Writes a dataframe as CSV with header
        /// </summary>
        public static void WriteAsCsvWithHeader(this DataFrame dataFrame, string path)
        {
            dataFrame
            .Repartition(1)
            .Write()
            .Mode(SaveMode.Overwrite).Option("header", true).Csv(path);
        }

    }
}