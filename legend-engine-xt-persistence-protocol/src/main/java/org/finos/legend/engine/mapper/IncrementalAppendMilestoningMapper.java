package org.finos.legend.engine.mapper;

import com.gs.alloy.components.logicalplan.datasets.Dataset;
import com.gs.alloy.components.milestoning.IncrementalAppendMilestoning;
import com.gs.alloy.components.milestoning.model.DuplicateDataHandlingStrategy;
import org.eclipse.collections.api.tuple.Pair;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.appendonly.AppendOnly;

import static org.finos.legend.engine.mapper.MilestoningMapper.*;

public class IncrementalAppendMilestoningMapper {

    /*
    TODO String[] pkFields,
    TODO: verify the DuplicateDataHandlingStrategy
     */
    public static IncrementalAppendMilestoning from(AppendOnly appendOnly,
                                                    Dataset mainDataSet,
                                                    Dataset stagingDataset) {
        Pair<Boolean, String> auditDetails = extractAuditDetails(appendOnly.auditing);
        boolean isUpdateBatchTimeEnabled = auditDetails.getOne();
        String updateBatchTimeField = auditDetails.getTwo();

        DuplicateDataHandlingStrategy duplicateDataHandlingStrategy = appendOnly.filterDuplicates ?
                DuplicateDataHandlingStrategy.FILTER_DUPLICATES : DuplicateDataHandlingStrategy.ALLOW_DUPLICATES;

        return IncrementalAppendMilestoning.builder()
                .mainDataSet(mainDataSet)
                .stagingDataSet(stagingDataset)
                .digestField(DIGEST_FIELD_DEFAULT)
                .pkFields(null)
                .cleanStagingData(CLEAN_STAGING_DATA_DEFAULT)
                .isStatsCollected(STATS_COLLECTION_DEFAULT)
                .isUpdateBatchTimeEnabled(isUpdateBatchTimeEnabled)
                .updateBatchTimeField(updateBatchTimeField)
                .isSchemaEvolutionEnabled(SCHEMA_EVOLUTION_DEFAULT)
                .duplicateDataHandlingStrategy(duplicateDataHandlingStrategy)
                .build();
    }
}
