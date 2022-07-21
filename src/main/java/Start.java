import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.PartialFunction;
import scala.collection.immutable.ArraySeq;


import java.util.ArrayList;

import
        scala.collection.Iterator;
//import scala.collection.immutable.List;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class Start {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\BigData\\Hadoop\\bin");

        SparkSession spark = SparkSession
                .builder()
                .appName("BigData")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> os = spark.read().//csv("src/main/resources/Erasmus.csv");
                format("csv").option("header", "true").load("src/main/resources/Erasmus.csv");
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

        //filtrare 3 coduri de tara care a primit studenti si care au fost codurile
        //de tari din care au provenit studentii si care a fost numarul lor
        //+afisare sortata
        Dataset<Row> udf = os;
        os =
                os.filter((os.col("Receiving Country Code").equalTo("RO"))
                                .or(os.col("Receiving Country Code").equalTo("LV"))
                                .or(os.col("Receiving Country Code").equalTo("AT")))
                        .groupBy(col("Receiving Country Code"), col("Sending Country Code"))
                        .count().sort("Receiving Country Code", "Sending Country Code");

        Properties prop = new Properties();
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "dorecuhuso42");

        os.write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost:5432/BigData", "erasmus", prop);

        Dataset<Row> LV = os.filter(os.col("Receiving Country Code").equalTo("LV"));
        LV.select(col("Sending Country Code"), col("count")).write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost:5432/BigData", "LV", prop);
        Dataset<Row> RO = os.filter(os.col("Receiving Country Code").equalTo("RO"));
        RO.select(col("Sending Country Code"), col("count")).write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost:5432/BigData", "RO", prop);
        Dataset<Row> AT = os.filter(os.col("Receiving Country Code").equalTo("AT"));
        AT.select(col("Sending Country Code"), col("count")).write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost:5432/BigData", "AT", prop);
//****************************************************************************************************************************************************//

        UDF1<scala.collection.Seq, String> groupData = new UDF1<scala.collection.Seq, String>() {
            @Override
            public String call(scala.collection.Seq s) throws Exception {

                System.out.println("************************************");
                List<String> stringList = new ArrayList<>();


                Iterator<String> iterator = s.iterator();
                while (iterator.hasNext()) {
                    System.out.println(stringList.add(iterator.next()));
                }
                //Stream<String> stringStream= stringList.stream();

                System.out.println(stringList);
                Map<String, Long> dictionar =
                        stringList.stream().collect(
                                Collectors.groupingBy(
                                        Function.identity(),
                                        HashMap::new,
                                        Collectors.counting()
                                )
                        );
                dictionar.forEach((key, value) -> System.out.println(key + ":" + value));
                System.out.println("************************************");
                //return String.valueOf(s.length());
                return "da";
            }
        };


        spark.udf().register("groupData", groupData, DataTypes.StringType);

        Dataset<Row> a =
                udf.filter((os.col("Receiving Country Code").equalTo("RO"))
                                .or(os.col("Receiving Country Code").equalTo("LV"))
                                .or(os.col("Receiving Country Code").equalTo("AT")))
                        .groupBy(col("Receiving Country Code")).agg(collect_list("Sending Country Code")
                                .as("Sending Country Code")).sort("Sending Country Code").withColumn("groupData", callUDF("groupData", col("Sending Country Code")));
        // .count().sort("Receiving Country Code","Sending Country Code").show(100);
//        a.printSchema();
//        a.show(100);
        a.write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost:5432/BigData", "verificare", prop);
    }
}
