package org.finos.legend.engine.mapper;

import com.gs.alloy.components.logicalplan.datasets.Dataset;
import com.gs.alloy.components.milestoning.UnitmeporalIncrementalMilestoning;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.delta.UnitemporalDelta;

import static org.finos.legend.engine.mapper.MilestoningMapper.*;

public class UnitemporalIncrementalMilestoningMapper {

    /*
    TODO String[] pkFields,
     */

    public static UnitmeporalIncrementalMilestoning from(UnitemporalDelta unitemporalDelta,
                                                         Dataset mainDataSet,
                                                         Dataset stagingDataset) {

        DeleteIndicator deleteIndicator = extractDeleteIndicator(unitemporalDelta.mergeStrategy);
        TransactionMilestoningDetails txMilestoningDetails = extractTransactionMilestoningDetails(unitemporalDelta.transactionMilestoning);
        return UnitmeporalIncrementalMilestoning.builder()
                .mainDataSet(mainDataSet)
                .stagingDataSet(stagingDataset)
                .digestField(DIGEST_FIELD_DEFAULT)
                .pkFields(null)
                .milestoningMode(txMilestoningDetails.milestoningMode)
                .batchIdInField(txMilestoningDetails.batchIdInName)
                .batchIdOutField(txMilestoningDetails.batchIdOutName)
                .batchTimeInField(txMilestoningDetails.batchTimeInName)
                .batchTimeOutField(txMilestoningDetails.batchTimeOutName)
                .hasDeleteIndicator(deleteIndicator.isEnabled)
                .deleteIndicatorField(deleteIndicator.deleteIndicatorField)
                .deleteIndicatorValues(deleteIndicator.deleteIndicatorValues)
                .cleanStagingData(CLEAN_STAGING_DATA_DEFAULT)
                .isStatsCollected(STATS_COLLECTION_DEFAULT)
                .isSchemaEvolutionEnabled(SCHEMA_EVOLUTION_DEFAULT)
                .build();
    }

}
