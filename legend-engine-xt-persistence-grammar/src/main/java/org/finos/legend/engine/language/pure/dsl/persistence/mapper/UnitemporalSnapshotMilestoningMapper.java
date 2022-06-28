// Copyright 2022 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.language.pure.dsl.persistence.mapper;

import com.gs.alloy.components.logicalplan.datasets.Dataset;
import com.gs.alloy.components.milestoning.UnitmeporalSnaphsotMilestoning;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.snapshot.UnitemporalSnapshot;

import static org.finos.legend.engine.language.pure.dsl.persistence.mapper.MilestoningMapper.DIGEST_FIELD_DEFAULT;
import static org.finos.legend.engine.language.pure.dsl.persistence.mapper.MilestoningMapper.STATS_COLLECTION_DEFAULT;
import static org.finos.legend.engine.language.pure.dsl.persistence.mapper.MilestoningMapper.CLEAN_STAGING_DATA_DEFAULT;
import static org.finos.legend.engine.language.pure.dsl.persistence.mapper.MilestoningMapper.SCHEMA_EVOLUTION_DEFAULT;

public class UnitemporalSnapshotMilestoningMapper
{
    public static UnitmeporalSnaphsotMilestoning from(UnitemporalSnapshot unitemporalSnapshot,
                                                      Dataset mainDataSet,
                                                      Dataset stagingDataset)
    {
        TransactionMilestoningDetails txMilestoningDetails = MilestoningMapper.extractTransactionMilestoningDetails(unitemporalSnapshot.transactionMilestoning);
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
