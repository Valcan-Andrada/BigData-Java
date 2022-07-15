import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

public class Start {
    public static void main(String[] args) throws AnalysisException {
        
        System.setProperty("hadoop.home.dir","C:\\BigData\\Hadoop\\bin");

        SparkSession spark = SparkSession
                .builder()
                .appName("BigData")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> os = spark.read().//csv("src/main/resources/Erasmus.csv");
                format("csv").option("header","true").load("src/main/resources/Erasmus.csv");
        os.printSchema();

        // os.show(20,false);
        // os.show();
        //os.select("_c1").show();
        //select everybody, but increment the age by 1
        //os.select(col("_c1").plus(10)).show();
        //df.select(col("name"), col("age").plus(1)).show();
        //select people older than 21
        // os.filter(col("_c1").gt(3)).show();
        //count people by age
        //os.groupBy("_c1").count().show();

        //RUNNING SQL QUERIES PROGRAMMALICALLY
        //register DataFrame as a sql temporary view
        //   os.createGlobalTempView("Erasmus");
        // Global temporary view is tied to a system preserved database `global_temp`
        // spark.sql("select * from global_temp.Erasmus").show();
        // *******************
        //os.groupBy("Receiving Country Code").equals();
        // os.show();



//       os.filter((os.col("Receiving Country Code").equalTo("RO"))
//                        .or(os.col("Receiving Country Code").equalTo("AT"))
//                        .or(os.col("Receiving Country Code").equalTo("BG")))
//                        .groupBy("Receiving Country Code")
//                        .count().show();

        os.filter((os.col("Receiving Country Code").equalTo("RO"))
                        .or(os.col("Receiving Country Code").equalTo("LV"))
                        .or(os.col("Receiving Country Code").equalTo("AT")))
                        .groupBy(col("Receiving Country Code"),col("Sending Country Code"))
                        .count().sort("Receiving Country Code","Sending Country Code").show(100);



    }
}
