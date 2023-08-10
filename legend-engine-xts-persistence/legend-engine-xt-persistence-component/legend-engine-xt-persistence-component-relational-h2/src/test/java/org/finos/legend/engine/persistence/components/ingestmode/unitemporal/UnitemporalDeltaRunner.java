package org.finos.legend.engine.persistence.components.ingestmode.unitemporal;

import org.finos.legend.engine.persistence.components.BaseTest;
import org.finos.legend.engine.persistence.components.TestUtils;
import org.finos.legend.engine.persistence.components.common.Datasets;
import org.finos.legend.engine.persistence.components.ingestmode.IngestMode;
import org.finos.legend.engine.persistence.components.ingestmode.UnitemporalDelta;
import org.finos.legend.engine.persistence.components.ingestmode.transactionmilestoning.BatchIdAndDateTime;
import org.finos.legend.engine.persistence.components.logicalplan.datasets.DatasetDefinition;
import org.finos.legend.engine.persistence.components.relational.api.IngestorResult;
import org.finos.legend.engine.persistence.components.relational.api.RelationalIngestor;
import org.finos.legend.engine.persistence.components.relational.executor.RelationalExecutor;
import org.finos.legend.engine.persistence.components.relational.h2.H2Sink;
import org.finos.legend.engine.persistence.components.relational.jdbc.JdbcConnection;
import org.finos.legend.engine.persistence.components.relational.jdbc.JdbcHelper;

import java.util.List;
import java.util.Map;

import static org.finos.legend.engine.persistence.components.TestUtils.*;

public class UnitemporalDeltaRunner extends BaseTest implements Runnable {

    private final String stagingSuffix;
    private String dataPath;
    DatasetDefinition mainTable = TestUtils.getDefaultMainTable();

    IngestMode getIngestMode() {
        UnitemporalDelta ingestMode = UnitemporalDelta.builder()
                .digestField(digestName)
                .transactionMilestoning(BatchIdAndDateTime.builder()
                        .batchIdInName(batchIdInName)
                        .batchIdOutName(batchIdOutName)
                        .dateTimeInName(batchTimeInName)
                        .dateTimeOutName(batchTimeOutName)
                        .build())
                .build();
        return ingestMode;
    }

    public UnitemporalDeltaRunner(String dataPath, String stagingSuffix) {
        this.dataPath = dataPath;
        this.stagingSuffix = stagingSuffix;
    }

    @Override
    public void run() {
        try
        {
            String updateQuery = "UPDATE batch_metadata " +
                    "set batch_status = 'CLAIMED' " +
                    "where " +
                    "table_name = 'main' and " +
                    "table_batch_id = (SELECT COALESCE(MAX(batch_metadata.\"table_batch_id\"),0) FROM batch_metadata as batch_metadata WHERE UPPER(batch_metadata.\"table_name\") = 'MAIN') " +
                    "and batch_status = 'CLAIMED'" ;

            String expectedMetadataTableIngestQuery = "INSERT INTO batch_metadata " +
                    "(\"table_name\", \"table_batch_id\", \"batch_status\")" +
                    " (SELECT 'main',(SELECT COALESCE(MAX(batch_metadata.\"table_batch_id\"),0)+1 FROM batch_metadata as batch_metadata " +
                    "WHERE UPPER(batch_metadata.\"table_name\") = 'MAIN'),'CLAIMED')";

            JdbcHelper h2Sink = JdbcHelper.of(H2Sink.createConnection(H2_USER_NAME, H2_PASSWORD, H2_JDBC_URL));
            RelationalExecutor executor = new RelationalExecutor(H2Sink.get(), h2Sink);

            executor.begin();
            Thread.sleep(Math.round(Math.random()*10));
            executor.getRelationalExecutionHelper().executeStatement(updateQuery);
            executor.getRelationalExecutionHelper().executeStatement(expectedMetadataTableIngestQuery);
            Thread.sleep(Math.round(Math.random()*10));
            executor.commit();

            Thread.sleep(Math.round(Math.random()*20));
            List<Map<String, Object>> batchMeta = h2Sink.executeQuery("select * from batch_metadata");
            System.out.println(String.format("%s : batchMeta : %s", Thread.currentThread().getName() , batchMeta));

            /*
            DatasetDefinition stagingTable = DatasetDefinition.builder()
                    .group(testSchemaName)
                    .name(stagingTableName + stagingSuffix)
                    .schema(getStagingSchema())
                    .build();

            createStagingTable(stagingTable);
            loadBasicStagingData(dataPath, stagingTableName + stagingSuffix);
            Datasets datasets = Datasets.of(mainTable, stagingTable);
            RelationalIngestor ingestor = RelationalIngestor.builder()
                    .ingestMode(getIngestMode())
                    .relationalSink(H2Sink.get())
                    .cleanupStagingData(true)
                    .collectStatistics(true)
                    .build();

            IngestorResult result = ingestor.performFullIngestion(JdbcConnection.of(h2Sink.connection()), datasets);
            System.out.println(String.format("%s : BatchId : %s", Thread.currentThread().getName() , result.batchId()));
            System.out.println(String.format("%s : stats : %s", Thread.currentThread().getName() , result.statisticByName()));
            System.out.println(String.format("%s : Ingestion ts : %s", Thread.currentThread().getName() , result.ingestionTimestampUTC()));

            List<Map<String, Object>> batchMeta = h2Sink.executeQuery("select * from batch_metadata");
            System.out.println(String.format("%s : batchMeta : %s", Thread.currentThread().getName() , batchMeta));

            List<Map<String, Object>> table = h2Sink.executeQuery("select * from TEST.main");
            System.out.println(String.format("%s : table : %s", Thread.currentThread().getName() , table));
             */
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    protected void loadBasicStagingData(String path, String tableName) throws Exception
    {
        validateFileExists(path);
        String loadSql = String.format("TRUNCATE TABLE \"TEST\".\"%s\";", tableName) +
                String.format("INSERT INTO \"TEST\".\"%s\"(id, name, income, start_time ,expiry_date, digest) ", tableName) +
                "SELECT CONVERT( \"id\",INT ), \"name\", CONVERT( \"income\", BIGINT), CONVERT( \"start_time\", DATETIME), CONVERT( \"expiry_date\", DATE), digest" +
                " FROM CSVREAD( '" + path + "', 'id, name, income, start_time, expiry_date, digest', NULL )";
        h2Sink.executeStatement(loadSql);
    }
}
