import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;

import java.util.*;

import scala.collection.Iterator;

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

        //*********************************************************************************************
        //TEMPORARY VIEW

        //We have 4 options for register DataFrame as a temporary view
        //1.createGlobalTempView
        //2.createOrReplaceGlobalTempView
        //3.createOrReplaceTempView
        //4.createTempView

        Properties prop = new Properties();
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "dorecuhuso42");

        os.write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost:5432/BigData", "erasmus", prop);


        //register the dataframe as a SQL temporary view
        os.createOrReplaceTempView("erasmus");

        spark.sql("SELECT * FROM erasmus where Receiving_Country_Code in ('LV','AT','RO') order by Receiving_Country_Code asc").write()
                .mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost:5432/BigData", "sql_result", prop);


//
//        Dataset<Row> namesDF = spark.sql("SELECT Participant_Age FROM erasmus WHERE Participant_Age BETWEEN 22 AND 25");
//        Dataset<String> namesDS = namesDF.map(new MapFunction<Row, String>() {
//            public String call(Row row) {
//                return row.getString(0);
//            }
//        }, Encoders.STRING());
//
//        namesDS.show(false);
//
//        register dataframe as a global temporary view
//        os.createGlobalTempView("my_table");
//        // Global temporary view is tied to a system preserved database `global_temp`
//        spark.sql("SELECT * FROM global_temp.my_table").show();
//        // Global temporary view is cross-session
//        spark.newSession().sql("SELECT * FROM global_temp.my_table").show();

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

        udfDataset = udfDataset.filter((udfDataset.col("Receiving_Country_Code").equalTo("RO"))
                        .or(udfDataset.col("Receiving_Country_Code").equalTo("LV"))
                        .or(udfDataset.col("Receiving_Country_Code").equalTo("AT")))
                .groupBy(col("Receiving_Country_Code")).agg(collect_list("Sending_Country_Code")
                        .as("Sending_Country_Code")).sort("Sending_Country_Code")
                .withColumn("groupingData", callUDF("groupingData", col("Receiving_Country_Code"), col("Sending_Country_Code")));

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

        os = os.filter(col("Receiving_Country_Code")
                        .isin(selected.toArray()))
                .groupBy(col("Receiving_Country_Code"), col("Sending_Country_Code"))
                .count().sort("Receiving_Country_Code", "Sending_Country_Code");

        saveToDatabase(os);
    }

    private static void saveToDatabase(Dataset<Row> os) {

        Properties prop = new Properties();
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "dorecuhuso42");

        os.write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost:5432/BigData", "erasmus", prop);

        Dataset<Row> LV = os.filter(os.col("Receiving_Country_Code").equalTo("LV"));
        LV.select(col("Sending_Country_Code"), col("count"))
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc("jdbc:postgresql://localhost:5432/BigData", "LV", prop);

        Dataset<Row> RO = os.filter(os.col("Receiving_Country_Code").equalTo("RO"));
        RO.select(col("Sending_Country_Code"), col("count"))
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc("jdbc:postgresql://localhost:5432/BigData", "RO", prop);

        Dataset<Row> AT = os.filter(os.col("Receiving_Country_Code").equalTo("AT"));
        AT.select(col("Sending_Country_Code"), col("count"))
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc("jdbc:postgresql://localhost:5432/BigData", "AT", prop);
    }
}
