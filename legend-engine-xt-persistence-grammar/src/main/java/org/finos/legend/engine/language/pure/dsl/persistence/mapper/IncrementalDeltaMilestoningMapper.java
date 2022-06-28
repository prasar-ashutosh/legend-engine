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
import com.gs.alloy.components.milestoning.IncrementalDeltaMilestoning;
import org.eclipse.collections.api.tuple.Pair;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.persister.ingestmode.delta.NontemporalDelta;

import static org.finos.legend.engine.language.pure.dsl.persistence.mapper.MilestoningMapper.DIGEST_FIELD_DEFAULT;
import static org.finos.legend.engine.language.pure.dsl.persistence.mapper.MilestoningMapper.STATS_COLLECTION_DEFAULT;
import static org.finos.legend.engine.language.pure.dsl.persistence.mapper.MilestoningMapper.CLEAN_STAGING_DATA_DEFAULT;
import static org.finos.legend.engine.language.pure.dsl.persistence.mapper.MilestoningMapper.SCHEMA_EVOLUTION_DEFAULT;

public class IncrementalDeltaMilestoningMapper
{

    /*
    TODO String[] pkFields,
    TODO Verify the DIGEST_FIELD_DEFAULT
    TODO Use the deleteIndicator in Incremental Delta
     */
    public static IncrementalDeltaMilestoning from(NontemporalDelta nontemporalDelta,
                                                   Dataset mainDataSet,
                                                   Dataset stagingDataset)
    {
        Pair<Boolean, String> auditDetails = MilestoningMapper.extractAuditDetails(nontemporalDelta.auditing);
        DeleteIndicator deleteIndicator = MilestoningMapper.extractDeleteIndicator(nontemporalDelta.mergeStrategy);

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
