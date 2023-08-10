package org.finos.legend.engine.persistence.components.ingestmode.unitemporal;

import org.finos.legend.engine.persistence.components.BaseTest;
import org.finos.legend.engine.persistence.components.logicalplan.LogicalPlan;
import org.finos.legend.engine.persistence.components.logicalplan.LogicalPlanFactory;
import org.finos.legend.engine.persistence.components.logicalplan.datasets.Dataset;
import org.finos.legend.engine.persistence.components.relational.SqlPlan;
import org.finos.legend.engine.persistence.components.relational.h2.H2Sink;
import org.finos.legend.engine.persistence.components.relational.transformer.RelationalTransformer;
import org.finos.legend.engine.persistence.components.util.MetadataDataset;
import org.finos.legend.engine.persistence.components.util.MetadataDatasetAbstract;
import org.junit.jupiter.api.Test;

public class UnitemporalConcurrentTest extends BaseTest {


    @Test
    public void test() throws InterruptedException {

        String expectedMetadataTableCreateQuery = "CREATE TABLE IF NOT EXISTS batch_metadata" +
                "(\"table_name\" VARCHAR(255)," +
                "\"batch_start_ts_utc\" DATETIME," +
                "\"batch_end_ts_utc\" DATETIME," +
                "\"batch_status\" VARCHAR(32)," +
                "\"table_batch_id\" INTEGER," +
                "\"staging_filters\" JSON)";
        h2Sink.executeStatement(expectedMetadataTableCreateQuery);

        h2Sink.executeStatement("insert into batch_metadata (table_name, table_batch_id, batch_status ) values ('main', 0, 'CLAIMED')");


        // Thread 1
        String path1 = "src/test/resources/data/unitemporal-incremental-milestoning/input/batch_id_and_time_based/without_delete_ind/staging_data_pass1.csv";
        Runnable r1 = new UnitemporalDeltaRunner(path1, "_thread1");
        Thread t1 = new Thread(r1);
        t1.start();


        // Thread 2
        String path2 = "src/test/resources/data/unitemporal-incremental-milestoning/input/batch_id_and_time_based/without_delete_ind/staging_data_pass2.csv";
        Runnable r2 = new UnitemporalDeltaRunner(path2, "_thread2");
        Thread t2 = new Thread(r2);
        t2.start();


        // Thread 2
        Runnable r3 = new UnitemporalDeltaRunner(path2, "_thread2");
        Thread t3 = new Thread(r3);
        t3.start();

        Thread.sleep(10000);
    }

}
