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
                .master("local[4]")
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", "./target/warehouse")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.allowMultipleContexts", "true")
                .config("spark.driver.port", 50500)
                .enableHiveSupport()
                .getOrCreate();
    }

    @Test
    public void queryHiveTable() {
        spark.sql("show databases").show();
    }


}
