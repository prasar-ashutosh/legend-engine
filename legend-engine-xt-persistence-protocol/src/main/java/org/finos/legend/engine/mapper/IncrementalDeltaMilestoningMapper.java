package org.finos.legend.engine.mapper;

import com.gs.alloy.components.logicalplan.datasets.Dataset;
import com.gs.alloy.components.milestoning.IncrementalDeltaMilestoning;
import org.eclipse.collections.api.tuple.Pair;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.delta.NontemporalDelta;

import static org.finos.legend.engine.mapper.MilestoningMapper.*;

public class IncrementalDeltaMilestoningMapper {

    /*
    TODO String[] pkFields,
    TODO Verify the DIGEST_FIELD_DEFAULT
    TODO Use the deleteIndicator in Incremental Delta
     */
    public static IncrementalDeltaMilestoning from(NontemporalDelta nontemporalDelta,
                                                   Dataset mainDataSet,
                                                   Dataset stagingDataset) {
        Pair<Boolean, String> auditDetails = extractAuditDetails(nontemporalDelta.auditing);
        DeleteIndicator deleteIndicator = extractDeleteIndicator(nontemporalDelta.mergeStrategy);

        boolean isUpdateBatchTimeEnabled = auditDetails.getOne();
        String updateBatchTimeField = auditDetails.getTwo();

        return IncrementalDeltaMilestoning.builder()
                .mainDataSet(mainDataSet)
                .stagingDataSet(stagingDataset)
                .digestField(DIGEST_FIELD_DEFAULT)
                .pkFields(null)
                .cleanStagingData(CLEAN_STAGING_DATA_DEFAULT)
                .isStatsCollected(STATS_COLLECTION_DEFAULT)
                .isUpdateBatchTimeEnabled(isUpdateBatchTimeEnabled)
                .updateBatchTimeField(updateBatchTimeField)
                .isSchemaEvolutionEnabled(SCHEMA_EVOLUTION_DEFAULT)
                .build();
    }
}
