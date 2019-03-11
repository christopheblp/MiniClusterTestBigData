package MiniClusters;

import java.io.IOException;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SparkJobsTest {

    private SparkSession spark;
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", folder.newFolder().getAbsolutePath())
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
