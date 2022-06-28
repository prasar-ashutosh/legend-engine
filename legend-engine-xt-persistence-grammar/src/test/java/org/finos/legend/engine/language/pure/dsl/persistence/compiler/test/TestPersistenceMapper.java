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

package org.finos.legend.engine.language.pure.dsl.persistence.compiler.test;

import com.gs.alloy.components.ansi.RelationalMilestoningOrchestrator;
import com.gs.alloy.components.ansi.utils.MilestoningStatistics;
import com.gs.alloy.components.h2.planner.sql.H2Executor;
import com.gs.alloy.components.logicalplan.datasets.Dataset;
import com.gs.alloy.components.milestoning.Milestoning;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.language.pure.compiler.test.TestCompilationFromGrammar;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.language.pure.dsl.persistence.mapper.DatasetMapper;
import org.finos.legend.engine.language.pure.dsl.persistence.mapper.MilestoningMapper;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.Persistence;
import org.finos.legend.pure.generated.Root_meta_pure_persistence_metamodel_Persistence;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement;
import org.junit.Test;

import static org.finos.legend.engine.language.pure.compiler.toPureGraph.HelperModelBuilder.getElementFullPath;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestPersistenceMapper extends TestCompilationFromGrammar.TestCompilationFromGrammarTestSuite
{

    @Override
    protected String getDuplicatedElementTestCode()
    {
        return "Class test::TestPersistence {}\n" +
                "\n" +
                "Class test::ModelClass {}\n" +
                "\n" +
                "###Persistence\n" +
                "\n" +
                "Persistence test::TestPersistence \n" +
                "{\n" +
                "  doc: 'This is test documentation.';\n" +
                "  trigger: Manual;\n" +
                "  service: test::Service;\n" +
                "  persister: Batch\n" +
                "  {\n" +
                "    sink: Relational\n" +
                "    {\n" +
                "      database: test::Database;\n" +
                "    }\n" +
                "    targetShape: Flat\n" +
                "    {\n" +
                "      targetName: 'TestDataset1';\n" +
                "      modelClass: test::ModelClass;\n" +
                "    }\n" +
                "    ingestMode: AppendOnly\n" +
                "    {\n" +
                "      auditing: None;\n" +
                "      filterDuplicates: false;\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
    }

    @Override
    protected String getDuplicatedElementTestExpectedErrorMessage()
    {
        return "COMPILATION error at [7:1-29:1]: Duplicated element 'test::TestPersistence'";
    }

    @Test
    public void flatShape() throws Exception
    {
        Pair<PureModelContextData, PureModel> result = test("Class test::Person\n" +
                "{\n" +
                "  name: String[1];\n" +
                "}\n" +
                "\n" +
                "Class test::ServiceResult\n" +
                "{\n" +
                "   deleted: String[1];\n" +
                "   dateTimeIn: String[1];\n" +
                "}\n" +
                "\n" +
                "###Mapping\n" +
                "Mapping test::Mapping ()\n" +
                "\n" +
                "###Service\n" +
                "Service test::Service \n" +
                "{\n" +
                "  pattern : 'test';\n" +
                "  documentation : 'test';\n" +
                "  autoActivateUpdates: true;\n" +
                "  execution: Single\n" +
                "  {\n" +
                "    query: src: test::Person[1]|$src.name;\n" +
                "    mapping: test::Mapping;\n" +
                "    runtime:\n" +
                "    #{\n" +
                "      connections: [];\n" +
                "    }#;\n" +
                "  }\n" +
                "  test: Single\n" +
                "  {\n" +
                "    data: 'test';\n" +
                "    asserts: [];\n" +
                "  }\n" +
                "}\n" +
                "\n" +
                "###Relational\n" +
                "Database test::Database\n" +
                "(\n" +
                "  Table personTable\n" +
                "  (\n" +
                "    ID INTEGER PRIMARY KEY,\n" +
                "    NAME VARCHAR(100)\n" +
                "  )\n" +
                ")" +
                "\n" +
                "###Persistence\n" +
                "\n" +
                "Persistence test::TestPersistence \n" +
                "{\n" +
                "  doc: 'This is test documentation.';\n" +
                "  trigger: Manual;\n" +
                "  service: test::Service;\n" +
                "  persister: Batch\n" +
                "  {\n" +
                "    sink: Relational\n" +
                "    {\n" +
                "      database: test::Database;" +
                "    }\n" +
                "    ingestMode: UnitemporalDelta\n" +
                "    {\n" +
                "      mergeStrategy: NoDeletes;\n" +
                "      transactionMilestoning: DateTime\n" +
                "      {\n" +
                "        dateTimeInName: 'IN_Z';\n" +
                "        dateTimeOutName: 'OUT_Z';\n" +
                "        derivation: SourceSpecifiesInDateTime\n" +
                "        {\n" +
                "          sourceDateTimeInField: 'dateTimeIn';\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "    targetShape: Flat\n" +
                "    {\n" +
                "      targetName: 'personTable';\n" +
                "      modelClass: test::ServiceResult;\n" +
                "    }\n" +
                "  }\n" +
                "}\n");

        PureModel model = result.getTwo();

        // persistence
        PackageableElement packageableElement = model.getPackageableElement("test::TestPersistence");
        assertNotNull(packageableElement);
        assertTrue(packageableElement instanceof Root_meta_pure_persistence_metamodel_Persistence);

        Root_meta_pure_persistence_metamodel_Persistence persistence = (Root_meta_pure_persistence_metamodel_Persistence) packageableElement;
        Persistence persistenceProtocol = ListIterate.detect(result.getOne().getElementsOfType(Persistence.class), ele -> ele.getPath().equals(getElementFullPath(persistence, model.getExecutionSupport())));

        String testData = "";
        Dataset targetDataset = DatasetMapper.getTargetDataset(persistence);
        Dataset stagingDataset = DatasetMapper.getStagingDataset(targetDataset,testData);
        Milestoning milestoning = MilestoningMapper.from(persistenceProtocol,targetDataset,stagingDataset);
        System.out.println(">> AWESOMENESS!! ");
        System.out.println(milestoning);

        RelationalMilestoningOrchestrator orchestrator = new RelationalMilestoningOrchestrator();
        MilestoningStatistics stats = orchestrator.execute(milestoning, new H2Executor());
        System.out.println(stats);

    }
}