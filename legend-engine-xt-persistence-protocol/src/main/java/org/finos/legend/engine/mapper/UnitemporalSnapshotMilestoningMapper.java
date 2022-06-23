package org.finos.legend.engine.mapper;

import com.gs.alloy.components.logicalplan.datasets.Dataset;
import com.gs.alloy.components.milestoning.UnitmeporalSnaphsotMilestoning;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.delta.UnitemporalDelta;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.snapshot.UnitemporalSnapshot;

import static org.finos.legend.engine.mapper.MilestoningMapper.*;

public class UnitemporalSnapshotMilestoningMapper {

    public static UnitmeporalSnaphsotMilestoning from(UnitemporalSnapshot unitemporalSnapshot,
                                                      Dataset mainDataSet,
                                                      Dataset stagingDataset) {

        TransactionMilestoningDetails txMilestoningDetails = extractTransactionMilestoningDetails(unitemporalSnapshot.transactionMilestoning);
        return UnitmeporalSnaphsotMilestoning.builder()
                .mainDataSet(mainDataSet)
                .stagingDataSet(stagingDataset)
                .digestField(DIGEST_FIELD_DEFAULT)
                .pkFields(null)
                .milestoningMode(txMilestoningDetails.milestoningMode)
                .batchIdInField(txMilestoningDetails.batchIdInName)
                .batchIdOutField(txMilestoningDetails.batchIdOutName)
                .batchTimeInField(txMilestoningDetails.batchTimeInName)
                .batchTimeOutField(txMilestoningDetails.batchTimeOutName)
                .cleanStagingData(CLEAN_STAGING_DATA_DEFAULT)
                .isStatsCollected(STATS_COLLECTION_DEFAULT)
                .isSchemaEvolutionEnabled(SCHEMA_EVOLUTION_DEFAULT)
                .build();
    }

}
