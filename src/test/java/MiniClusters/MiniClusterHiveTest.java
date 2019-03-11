package MiniClusters;


import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class MiniClusterHiveTest {

    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Before
    public void createDatabaseAndTable() {
        shell.execute("CREATE DATABASE source_db");
        shell.execute("CREATE TABLE source_db.survey (" +
                "submittedTime STRING," +
                "age STRING," +
                "gender STRING," +
                "country STRING," +
                "state STRING," +
                "self_employed STRING," +
                "family_history STRING," +
                "treatment STRING," +
                "work_interfere STRING," +
                "no_employees STRING," +
                "remote_work STRING," +
                "tech_company STRING," +
                "benefits STRING," +
                "care_options STRING," +
                "wellness_program STRING," +
                "seek_help STRING," +
                "anonymity STRING," +
                "leave STRING," +
                "mental_health_consequence STRING," +
                "phys_health_consequence STRING," +
                "coworkers STRING," +
                "supervisor STRING," +
                "mental_health_interview STRING," +
                "phys_health_interview STRING," +
                "mental_vs_physical STRING," +
                "obs_consequence STRING," +
                "comments STRING) " +
                "ROW FORMAT DELIMITED " +
                "FIELDS TERMINATED BY ',' " +
                "STORED AS TEXTFILE " +
                "LOCATION '/home/finaxys/IdeaProjects/MiniClusterTestBigData/src/test/resources/datasets/'");
    }

    @Before
    public void createORCTable() {
        shell.execute("CREATE TABLE source_db.survey2 (" +
                "submittedTime STRING," +
                "age STRING," +
                "gender STRING," +
                "country STRING," +
                "state STRING," +
                "self_employed STRING," +
                "family_history STRING," +
                "treatment STRING," +
                "work_interfere STRING," +
                "no_employees STRING," +
                "remote_work STRING," +
                "tech_company STRING," +
                "benefits STRING," +
                "care_options STRING," +
                "wellness_program STRING," +
                "seek_help STRING," +
                "anonymity STRING," +
                "leave STRING," +
                "mental_health_consequence STRING," +
                "phys_health_consequence STRING," +
                "coworkers STRING," +
                "supervisor STRING," +
                "mental_health_interview STRING," +
                "phys_health_interview STRING," +
                "mental_vs_physical STRING," +
                "obs_consequence STRING," +
                "comments STRING) " +
                "ROW FORMAT DELIMITED " +
                "FIELDS TERMINATED BY ',' " +
                "STORED AS ORC tblproperties (\"orc.compress\"=\"ZLIB\"); ");

    }

    @Before
    public void createParquetTable() {
        shell.execute("CREATE TABLE source_db.survey3 " +
                "STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")\n" +
                " AS SELECT * FROM source_db.survey;");
    }

    /**
     * We use temporary table survey to load the orc table survey2
     */
    @Test
    public void loadOrcSurvey2Table() {
        shell.execute("INSERT INTO TABLE source_db.survey2 SELECT * from source_db.survey");
    }

    @Ignore("Use for simple test without external file")
    public void simpleInit() {
        shell.execute("CREATE DATABASE source_db");
        shell.execute("CREATE TABLE source_db.test (" +
                "a STRING," +
                "b STRING ) ");

    }

    @Ignore
    public void simpleInsertionDataIntoTable() {
        shell.insertInto("source_db", "test")
                .withAllColumns()
                .addRow("bim", "bap")
                .commit();

        printResult(shell.executeStatement("select * from source_db.test"));
    }

    @Test
    public void executeQuery() {

        List<Object[]> result = shell.executeStatement("select * from source_db.survey where age=37");
        List<Object[]> result2 = shell.executeStatement("select * from source_db.survey where age=12");

        Assert.assertEquals(43L, result.size());
        Assert.assertEquals(0L, result2.size());
    }

    @Test
    public void insertDataIntoTable() {
        shell.insertInto("source_db", "survey")
                .withAllColumns()
                .addRow("2019-03-01 09:29:31",
                        17,
                        "Male",
                        "France",
                        "IL",
                        "NA",
                        "No",
                        "Yes",
                        "Often",
                        "6-25",
                        "No",
                        "Yes",
                        "Yes",
                        "Not sure",
                        "No",
                        "Yes",
                        "Yes",
                        "Somewhat easy",
                        "No",
                        "No",
                        "Some of them",
                        "Yes",
                        "No",
                        "Maybe",
                        "Yes",
                        "No",
                        "NA"
                )
                .commit();
        printResult(shell.executeStatement("select * from source_db.survey where age=17"));
    }

    private void printResult(List<Object[]> result) {
        System.out.println(String.format("Result from %s:", name.getMethodName()));
        result.stream().map(Arrays::asList).forEach(System.out::println);
    }
}