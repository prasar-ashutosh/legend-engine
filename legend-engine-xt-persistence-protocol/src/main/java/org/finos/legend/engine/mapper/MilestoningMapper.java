package org.finos.legend.engine.mapper;

import com.gs.alloy.components.logicalplan.datasets.Dataset;
import com.gs.alloy.components.milestoning.*;
import com.gs.alloy.components.milestoning.model.MilestoningMode;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.Persistence;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.BatchPersister;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.Persister;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.auditing.Auditing;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.auditing.DateTimeAuditing;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.IngestMode;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.appendonly.AppendOnly;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.delta.BitemporalDelta;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.delta.NontemporalDelta;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.delta.UnitemporalDelta;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.delta.merge.DeleteIndicatorMergeStrategy;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.delta.merge.MergeStrategy;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.snapshot.BitemporalSnapshot;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.snapshot.NontemporalSnapshot;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.snapshot.UnitemporalSnapshot;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.transactionmilestoning.BatchIdAndDateTimeTransactionMilestoning;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.transactionmilestoning.BatchIdTransactionMilestoning;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.transactionmilestoning.DateTimeTransactionMilestoning;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.transactionmilestoning.TransactionMilestoning;

import java.util.List;

public class MilestoningMapper {

    public static final boolean CLEAN_STAGING_DATA_DEFAULT = true;
    public static final boolean STATS_COLLECTION_DEFAULT = true;
    public static final boolean SCHEMA_EVOLUTION_DEFAULT = false;
    public static final String DIGEST_FIELD_DEFAULT = "DIGEST";
    public static final String BATCH_ID_IN_FIELD_DEFAULT = "BATCH_IN";
    public static final String BATCH_ID_OUT_FIELD_DEFAULT = "BATCH_OUT";
    public static final String BATCH_TIME_IN_FIELD_DEFAULT = "BATCH_TIME_IN";
    public static final String BATCH_TIME_OUT_FIELD_DEFAULT = "BATCH_TIME_OUT";

    public static Milestoning from(Persistence persistence) throws Exception {
        IngestMode mode = getIngestMode(persistence);
        Milestoning milestoning = null;
        if (mode != null) {
            milestoning = extract(mode);
        }
        return milestoning;
    }

    private static IngestMode getIngestMode(Persistence persistence) throws Exception {
        Persister persister = persistence.persister;
        if (persister instanceof BatchPersister) {
            BatchPersister batchPersister = (BatchPersister) persister;
            return batchPersister.ingestMode;
        }
        throw new Exception("Only BatchPersister has Ingest Mode");
    }

    private static Milestoning extract(IngestMode ingestMode) throws Exception {
        Dataset mainDataSet = null;
        Dataset stagingDataset = null;

        if (ingestMode instanceof NontemporalSnapshot) {
            return SnapshotMilestoningMapper.from((NontemporalSnapshot) ingestMode, mainDataSet, stagingDataset);
        }
        if (ingestMode instanceof AppendOnly) {
            return IncrementalAppendMilestoningMapper.from((AppendOnly) ingestMode, mainDataSet, stagingDataset);
        }
        if (ingestMode instanceof NontemporalDelta) {
            return IncrementalDeltaMilestoningMapper.from((NontemporalDelta) ingestMode, mainDataSet, stagingDataset);
        }
        if (ingestMode instanceof UnitemporalSnapshot) {
            return UnitemporalSnapshotMilestoningMapper.from((UnitemporalSnapshot) ingestMode, mainDataSet, stagingDataset);
        }
        if (ingestMode instanceof UnitemporalDelta) {
            return UnitemporalIncrementalMilestoningMapper.from((UnitemporalDelta) ingestMode, mainDataSet, stagingDataset);
        }
        if (ingestMode instanceof BitemporalSnapshot) {
            // TODO
        }
        if (ingestMode instanceof BitemporalDelta) {
            // TODO
        }
        throw new Exception("Unsupported Ingest mode");
    }

    public static Pair<Boolean, String> extractAuditDetails(Auditing auditing) {
        boolean isAuditEnabled = false;
        String auditTimeFieldName = null;
        if (auditing instanceof DateTimeAuditing) {
            isAuditEnabled = true;
            DateTimeAuditing dateTimeAuditing = (DateTimeAuditing) auditing;
            auditTimeFieldName = dateTimeAuditing.dateTimeName;
        }
        return Tuples.pair(isAuditEnabled, auditTimeFieldName);
    }

    public static DeleteIndicator extractDeleteIndicator(MergeStrategy mergeStrategy) {
        boolean isDeleteIndicatorEnabled = false;
        String deleteIndicatorField = null;
        String[] deleteIndicatorValues = null;
        if (mergeStrategy instanceof DeleteIndicatorMergeStrategy) {
            isDeleteIndicatorEnabled = true;
            DeleteIndicatorMergeStrategy deleteIndicatorMergeStrategy = (DeleteIndicatorMergeStrategy) mergeStrategy;
            deleteIndicatorField = deleteIndicatorMergeStrategy.deleteField;
            List<String> deleteValues = deleteIndicatorMergeStrategy.deleteValues;
            deleteIndicatorValues = new String[deleteValues.size()];
            deleteValues.toArray(deleteIndicatorValues);
        }
        return new DeleteIndicator(isDeleteIndicatorEnabled, deleteIndicatorField, deleteIndicatorValues);
    }

    public static TransactionMilestoningDetails extractTransactionMilestoningDetails(TransactionMilestoning transactionMilestoning) {
        String batchIdInName = null;
        String batchIdOutName = null;
        String batchTimeInName = null;
        String batchTimeOutName = null;
        MilestoningMode mode = null;
        if (transactionMilestoning instanceof BatchIdTransactionMilestoning) {
            BatchIdTransactionMilestoning txMilestoning = (BatchIdTransactionMilestoning) transactionMilestoning;
            batchIdInName = txMilestoning.batchIdInName;
            batchIdOutName = txMilestoning.batchIdOutName;
            mode = MilestoningMode.BATCH_ID_BASED;
        } else if (transactionMilestoning instanceof BatchIdAndDateTimeTransactionMilestoning) {
            BatchIdAndDateTimeTransactionMilestoning txMilestoning = (BatchIdAndDateTimeTransactionMilestoning) transactionMilestoning;
            batchIdInName = txMilestoning.batchIdInName;
            batchIdOutName = txMilestoning.batchIdOutName;
            batchTimeInName = txMilestoning.dateTimeInName;
            batchTimeOutName = txMilestoning.dateTimeOutName;
            mode = MilestoningMode.BATCH_ID_AND_TIME_BASED;
        } else if (transactionMilestoning instanceof DateTimeTransactionMilestoning) {
            DateTimeTransactionMilestoning txMilestoning = (DateTimeTransactionMilestoning) transactionMilestoning;
            batchTimeInName = txMilestoning.dateTimeInName;
            batchTimeOutName = txMilestoning.dateTimeOutName;
            mode = MilestoningMode.TIME_BASED;
        }
        batchIdInName = batchIdInName == null ? BATCH_ID_IN_FIELD_DEFAULT : batchIdInName;
        batchIdOutName = batchIdOutName == null ? BATCH_ID_OUT_FIELD_DEFAULT : batchIdOutName;
        batchTimeInName = batchTimeInName == null ? BATCH_TIME_IN_FIELD_DEFAULT : batchTimeInName;
        batchTimeOutName = batchTimeOutName == null ? BATCH_TIME_OUT_FIELD_DEFAULT : batchTimeOutName;
        return new TransactionMilestoningDetails(mode, batchIdInName, batchIdOutName, batchTimeInName, batchTimeOutName);
    }
}
