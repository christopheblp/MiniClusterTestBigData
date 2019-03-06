package MiniClusters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class MiniClusterHadoopHDFS {

    private static final String CLUSTER_1 = "cluster1"; // name of the cluster


    private File testDataPath; // data directory for the cluster
    private Configuration conf; // Instantiate a new Configuration object for your cluster
    private MiniDFSCluster cluster;
    private FileSystem fs;


    /**
     * Start the minicluster
     *
     * @throws Exception
     */

    @Before
    public void setUp() throws Exception {
        System.out.println("Starting cluster ...");
        testDataPath = new File(PathUtils.getTestDir(getClass()),
                "miniclusters");

        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        conf = new HdfsConfiguration();

        File testDataCluster1 = new File(testDataPath, CLUSTER_1);
        String c1Path = testDataCluster1.getAbsolutePath();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
        cluster = new MiniDFSCluster.Builder(conf).build();

        fs = FileSystem.get(conf);
    }


    /**
     * Shutdown the cluster
     *
     * @throws Exception
     */

    @After
    public void tearDown() throws Exception {
        System.out.println("Shutting down the cluster");
        Path dataDir = new Path(
                testDataPath.getParentFile().getParentFile().getParent());
        fs.delete(dataDir, true);
        File rootTestFile = new File(testDataPath.getParentFile().getParentFile().getParent());
        String rootTestDir = rootTestFile.getAbsolutePath();
        Path rootTestPath = new Path(rootTestDir);
        LocalFileSystem localFileSystem = FileSystem.getLocal(conf);
        localFileSystem.delete(rootTestPath, true);
        cluster.shutdown();
    }

    @Test
    public void init() {
        System.out.println("rien");
    }

}
