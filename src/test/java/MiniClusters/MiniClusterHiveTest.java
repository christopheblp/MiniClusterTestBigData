package MiniClusters;


import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
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
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.survey (")
                .append("submittedTime STRING,")
                .append("age STRING,")
                .append("gender STRING,")
                .append("country STRING,")
                .append("state STRING,")
                .append("self_employed STRING,")
                .append("family_history STRING,")
                .append("treatment STRING,")
                .append("work_interfere STRING,")
                .append("no_employees STRING,")
                .append("remote_work STRING,")
                .append("tech_company STRING,")
                .append("benefits STRING,")
                .append("care_options STRING,")
                .append("wellness_program STRING,")
                .append("seek_help STRING,")
                .append("anonymity STRING,")
                .append("leave STRING,")
                .append("mental_health_consequence STRING,")
                .append("phys_health_consequence STRING,")
                .append("coworkers STRING,")
                .append("supervisor STRING,")
                .append("mental_health_interview STRING,")
                .append("phys_health_interview STRING,")
                .append("mental_vs_physical STRING,")
                .append("obs_consequence STRING,")
                .append("comments STRING) ")
                .append("ROW FORMAT DELIMITED ")
                .append("FIELDS TERMINATED BY ',' ")
                .append("STORED AS TEXTFILE ")
                .append("LOCATION '/home/finaxys/IdeaProjects/MiniClusterTestBigData/src/test/resources/datasets/'")
                .toString());
    }

    @Disabled("Use for simple test without external file")
    public void simpleInit() {
        shell.execute("CREATE DATABASE source_db");
        shell.execute("CREATE TABLE source_db.test (" +
                "a STRING," +
                "b STRING ) ");

    }

    @Disabled
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
