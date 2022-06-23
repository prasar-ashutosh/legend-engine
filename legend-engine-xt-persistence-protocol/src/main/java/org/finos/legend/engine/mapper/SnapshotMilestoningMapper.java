package org.finos.legend.engine.mapper;

import com.gs.alloy.components.logicalplan.datasets.Dataset;
import com.gs.alloy.components.milestoning.SnapshotMilestoning;
import org.eclipse.collections.api.tuple.Pair;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.snapshot.NontemporalSnapshot;

import static org.finos.legend.engine.mapper.MilestoningMapper.*;

public class SnapshotMilestoningMapper {

    public static SnapshotMilestoning from(NontemporalSnapshot nontemporalSnapshot, Dataset mainDataSet,
                                           Dataset stagingDataset) {
        Pair<Boolean, String> auditDetails = extractAuditDetails(nontemporalSnapshot.auditing);
        boolean isUpdateBatchTimeEnabled = auditDetails.getOne();
        String updateBatchTimeField = auditDetails.getTwo();

        return SnapshotMilestoning.builder()
                .mainDataSet(mainDataSet)
                .stagingDataSet(stagingDataset)
                .cleanStagingData(CLEAN_STAGING_DATA_DEFAULT)
                .isStatsCollected(STATS_COLLECTION_DEFAULT)
                .isUpdateBatchTimeEnabled(isUpdateBatchTimeEnabled)
                .updateBatchTimeField(updateBatchTimeField)
                .isSchemaEvolutionEnabled(SCHEMA_EVOLUTION_DEFAULT)
                .build();
    }
}
