package MiniClusters;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

public class SparkJobsTest {

    private SparkSession spark;

    @Before
    public void setUp() {
        spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.master", "local")
                .enableHiveSupport()
                .getOrCreate();
    }

    @Test
    public void queryHiveTable() {
        spark.sqlContext().sql("show databases").show();
    }


}
