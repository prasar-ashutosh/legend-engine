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

import com.gs.alloy.components.milestoning.model.MilestoningMode;

public class TransactionMilestoningDetails
{
    public MilestoningMode milestoningMode;
    public String batchIdInName;
    public String batchIdOutName;
    public String batchTimeInName;
    public String batchTimeOutName;

    public TransactionMilestoningDetails(MilestoningMode milestoningMode, String batchIdInName,
                                         String batchIdOutName, String batchTimeInName, String batchTimeOutName)
    {
        this.milestoningMode = milestoningMode;
        this.batchIdInName = batchIdInName;
        this.batchIdOutName = batchIdOutName;
        this.batchTimeInName = batchTimeInName;
        this.batchTimeOutName = batchTimeOutName;
    }

}
