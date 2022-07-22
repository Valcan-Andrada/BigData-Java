import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;


import java.util.*;

import
        scala.collection.Iterator;
//import scala.collection.immutable.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class Start {

    public static void main(String[] args) throws AnalysisException {

        System.setProperty("hadoop.home.dir", "C:\\BigData\\Hadoop\\bin");

        SparkSession spark = SparkSession
                .builder()
                .appName("BigData")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> os = spark.read().//csv("src/main/resources/Erasmus.csv");
                format("csv").option("header", "true").load("src/main/resources/Erasmus.csv");

        dataProcessing(os);

//       os.filter((os.col("Receiving Country Code").equalTo("RO"))
//                        .or(os.col("Receiving Country Code").equalTo("AT"))
//                        .or(os.col("Receiving Country Code").equalTo("BG")))
//                        .groupBy("Receiving Country Code")
//                        .count().show();


        UDF(os, spark);

        os.createOrReplaceTempView("eramus");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM eramus");

        sqlDF.show();

    }

    private static void UDF(Dataset<Row> udfDataset, SparkSession spark) {

        System.setProperty("hadoop.home.dir", "C:\\BigData\\Hadoop\\bin");

        UDF2<String, Seq, String> groupingData = new UDF2<String, Seq, String>() {
            @Override
            public String call(String receivingCountryCode, scala.collection.Seq sendingCountryCode) throws Exception {

                List<String> stringList = new ArrayList<>();

                Iterator<String> iterator = sendingCountryCode.iterator();

                while (iterator.hasNext()) {
                    System.out.println(stringList.add(iterator.next()));
                }

                Map<String, Long> dictionar =
                        stringList.stream().collect(
                                Collectors.groupingBy(
                                        Function.identity(),
                                        HashMap::new,
                                        Collectors.counting()
                                )
                        );

                dictionar.forEach((key, value) -> System.out.println(key + ":" + value));

                return "da";
            }
        };

        spark.udf().register("groupingData", groupingData, DataTypes.StringType);

        udfDataset = udfDataset.filter((udfDataset.col("Receiving Country Code").equalTo("RO"))
                        .or(udfDataset.col("Receiving Country Code").equalTo("LV"))
                        .or(udfDataset.col("Receiving Country Code").equalTo("AT")))
                .groupBy(col("Receiving Country Code")).agg(collect_list("Sending Country Code")
                        .as("Sending Country Code")).sort("Sending Country Code")
                .withColumn("groupingData", callUDF("groupingData", col("Receiving Country Code"), col("Sending Country Code")));

        udfDataset.show(false);
    }

    private static void dataProcessing(Dataset<Row> os) {

        System.setProperty("hadoop.home.dir", "C:\\BigData\\Hadoop\\bin");

        //filtrare 3 coduri de tara care a primit studenti si care au fost codurile
        //de tari din care au provenit studentii si care a fost numarul lor
        //+afisare sortata

//        os = os.filter((os.col("Receiving Country Code").equalTo("RO"))
//                        .or(os.col("Receiving Country Code").equalTo("LV"))
//                        .or(os.col("Receiving Country Code").equalTo("AT")))
//                        .groupBy(col("Receiving Country Code"), col("Sending Country Code"))
//                        .count().sort("Receiving Country Code", "Sending Country Code");

        //isin
        java.util.List<String> selected = new ArrayList<>();
        selected.add("AT");
        selected.add("RO");
        selected.add("LV");

        os = os.filter(col("Receiving Country Code")
                        .isin(selected.toArray()))
                        .groupBy(col("Receiving Country Code"), col("Sending Country Code"))
                        .count().sort("Receiving Country Code", "Sending Country Code");

        saveToDatabase(os);
    }

    private static void saveToDatabase(Dataset<Row> os) {

        Properties prop = new Properties();
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "dorecuhuso42");

        os.write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost:5432/BigData", "erasmus", prop);

        Dataset<Row> LV = os.filter(os.col("Receiving Country Code").equalTo("LV"));
        LV.select(col("Sending Country Code"), col("count"))
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc("jdbc:postgresql://localhost:5432/BigData", "LV", prop);

        Dataset<Row> RO = os.filter(os.col("Receiving Country Code").equalTo("RO"));
        RO.select(col("Sending Country Code"), col("count"))
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc("jdbc:postgresql://localhost:5432/BigData", "RO", prop);

        Dataset<Row> AT = os.filter(os.col("Receiving Country Code").equalTo("AT"));
        AT.select(col("Sending Country Code"), col("count"))
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc("jdbc:postgresql://localhost:5432/BigData", "AT", prop);
    }
}
