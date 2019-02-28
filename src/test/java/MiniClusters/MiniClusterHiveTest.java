package MiniClusters;


import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class MiniClusterHiveTest {
    @HiveSQL(files = {})
    private HiveShell shell;

    @Before
    public void createDatabaseAndTable() {
        shell.execute("CREATE DATABASE source_db");
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.survey (")
                .append("submittedTime TIMESTAMP,")
                .append("age INT,")
                .append("gender STRING,")
                .append("country STRING,")
                .append("state STRING,")
                .append("self_employed STRING,")
                .append("family_history STRING,")
                .append("treatment STRING,")
                .append("work_interfere STRING,")
                .append("no_employees STRING,")
                .append("remote_work STRING,")
                .append("tech_company BOOLEAN,")
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

    @Test
    public void executeQuery() {

        List<Object[]> result = shell.executeStatement("" +
                "select treatment" +
                "from source_db.survey " +
                "where remote_work='Yes'");
        Assert.assertEquals(43L, result.size());

    }


}
