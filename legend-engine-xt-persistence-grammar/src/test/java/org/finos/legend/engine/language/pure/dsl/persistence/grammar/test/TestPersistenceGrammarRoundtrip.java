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

package org.finos.legend.engine.language.pure.dsl.persistence.grammar.test;

import org.finos.legend.engine.language.pure.compiler.Compiler;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.language.pure.grammar.test.TestGrammarRoundtrip;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.v1.model.test.result.TestResult;
import org.finos.legend.engine.shared.core.deployment.DeploymentMode;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_Service;
import org.junit.Test;

import java.util.List;

public class TestPersistenceGrammarRoundtrip extends TestGrammarRoundtrip.TestGrammarRoundtripTestSuite {
    @Test
    public void persistenceFlat() {
        test("###Persistence\n" +
                "import test::*;\n" +
                "Persistence test::TestPersistence\n" +
                "{\n" +
                "  doc: 'test doc';\n" +
                "  trigger: Manual;\n" +
                "  service: test::service::Service;\n" +
                "  persister: Batch\n" +
                "  {\n" +
                "    sink: Relational\n" +
                "    {\n" +
                "      connection:\n" +
                "      #{\n" +
                "        JsonModelConnection\n" +
                "        {\n" +
                "          class: org::dxl::Animal;\n" +
                "          url: 'my_url2';\n" +
                "        }\n" +
                "      }#\n" +
                "    }\n" +
                "    ingestMode: BitemporalSnapshot\n" +
                "    {\n" +
                "      transactionMilestoning: BatchId\n" +
                "      {\n" +
                "        batchIdInName: 'batchIdIn';\n" +
                "        batchIdOutName: 'batchIdOut';\n" +
                "      }\n" +
                "      validityMilestoning: DateTime\n" +
                "      {\n" +
                "        dateTimeFromName: 'FROM_Z';\n" +
                "        dateTimeThruName: 'THRU_Z';\n" +
                "        derivation: SourceSpecifiesFromAndThruDateTime\n" +
                "        {\n" +
                "          sourceDateTimeFromField: sourceFrom;\n" +
                "          sourceDateTimeThruField: sourceThru;\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "    targetShape: Flat\n" +
                "    {\n" +
                "      modelClass: test::ModelClass;\n" +
                "      targetName: 'TestDataset1';\n" +
                "      partitionFields: [propertyA, propertyB];\n" +
                "      deduplicationStrategy: MaxVersion\n" +
                "      {\n" +
                "        versionField: version;\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "  notifier:\n" +
                "  {\n" +
                "    notifyees:\n" +
                "    [\n" +
                "      Email\n" +
                "      {\n" +
                "        address: 'x.y@z.com';\n" +
                "      },\n" +
                "      PagerDuty\n" +
                "      {\n" +
                "        url: 'https://x.com';\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}\n");
    }

    @Test
    public void testPersistenceTestSuite() {
        String persistenceCodeBlock = "###Persistence\n" +
                "import test::*;\n" +
                "Persistence test::TestPersistence\n" +
                "{\n" +
                "  doc: 'test doc';\n" +
                "  trigger: Manual;\n" +
                "  service: test::service::Service;\n" +
                "  persister: Batch\n" +
                "  {\n" +
                "    sink: Relational\n" +
                "    {\n" +
                "      connection:\n" +
                "      #{\n" +
                "        JsonModelConnection\n" +
                "        {\n" +
                "          class: org::dxl::Animal;\n" +
                "          url: 'my_url2';\n" +
                "        }\n" +
                "      }#\n" +
                "    }\n" +
                "    ingestMode: BitemporalSnapshot\n" +
                "    {\n" +
                "      transactionMilestoning: BatchId\n" +
                "      {\n" +
                "        batchIdInName: 'batchIdIn';\n" +
                "        batchIdOutName: 'batchIdOut';\n" +
                "      }\n" +
                "      validityMilestoning: DateTime\n" +
                "      {\n" +
                "        dateTimeFromName: 'FROM_Z';\n" +
                "        dateTimeThruName: 'THRU_Z';\n" +
                "        derivation: SourceSpecifiesFromAndThruDateTime\n" +
                "        {\n" +
                "          sourceDateTimeFromField: sourceFrom;\n" +
                "          sourceDateTimeThruField: sourceThru;\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "    targetShape: Flat\n" +
                "    {\n" +
                "      modelClass: test::ModelClass;\n" +
                "      targetName: 'TestDataset1';\n" +
                "      partitionFields: [propertyA, propertyB];\n" +
                "      deduplicationStrategy: MaxVersion\n" +
                "      {\n" +
                "        versionField: version;\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "  notifier:\n" +
                "  {\n" +
                "    notifyees:\n" +
                "    [\n" +
                "      Email\n" +
                "      {\n" +
                "        address: 'x.y@z.com';\n" +
                "      },\n" +
                "      PagerDuty\n" +
                "      {\n" +
                "        url: 'https://x.com';\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "%s" +
                "}\n";

        //Test Empty TestSuite
        String emptyTestSuite = "  testSuites:\n" +
                "  [\n" +
                "\n" +
                "  ]\n";

        String persistenceCodeWithEmptyTestSuite = String.format(persistenceCodeBlock, emptyTestSuite);
        test(persistenceCodeWithEmptyTestSuite);

        //Test Single TestSuite with data
        String testSuiteWithData = "  testSuites:\n" +
                "  [\n" +
                "    testSuite1:\n" +
                "    {\n" +
                "      data:\n" +
                "      [\n" +
                "        connections:\n" +
                "        [\n" +
                "          connection1:\n" +
                "            ExternalFormat\n" +
                "            #{\n" +
                "              contentType: 'application/x.flatdata';\n" +
                "              data: 'FIRST_NAME,LAST_NAME\\nFred,Bloggs\\nJane,Doe';\n" +
                "            }#\n" +
                "        ]\n" +
                "      ]\n" +
                "      tests:\n" +
                "      [\n" +
                "        test1:\n" +
                "        {\n" +
                "          asserts:\n" +
                "          [\n" +
                "            assert1:\n" +
                "              EqualToJson\n" +
                "              #{\n" +
                "                expected : \n" +
                "                  ExternalFormat\n" +
                "                  #{\n" +
                "                    contentType: 'application/json';\n" +
                "                    data: '{Age:12, Name:\"dummy\"}';\n" +
                "                  }#;\n" +
                "              }#\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n";

        String persistenceCodeWithTestSuiteWithData = String.format(persistenceCodeBlock, testSuiteWithData);
        //test(persistenceCodeWithTestSuiteWithData);

//        ServiceTestableRunnerExtension serviceTestableRunnerExtension = new ServiceTestableRunnerExtension();
//
//        PureModelContextData modelDataWithReferenceData = PureGrammarParser.newInstance().parseModel(persistenceCodeWithTestSuiteWithData);
//        PureModel pureModelWithReferenceData = Compiler.compile(modelDataWithReferenceData, DeploymentMode.TEST, null);
//
//        Root_meta_legend_service_metamodel_Service serviceWithReferenceData = (Root_meta_legend_service_metamodel_Service) pureModelWithReferenceData.getPackageableElement("testServiceStoreTestSuites::TestService");
//        List<TestResult> serviceStoreTestResults = serviceTestableRunnerExtension.executeAllTest(serviceWithReferenceData, pureModelWithReferenceData, modelDataWithReferenceData);
    }

    @Test
    public void persistenceMultiFlat() {
        test("###Persistence\n" +
                "import test::*;\n" +
                "Persistence test::TestPersistence\n" +
                "{\n" +
                "  doc: 'test doc';\n" +
                "  trigger: Manual;\n" +
                "  service: test::service::Service;\n" +
                "  persister: Batch\n" +
                "  {\n" +
                "    sink: ObjectStorage\n" +
                "    {\n" +
                "      binding: test::Binding;\n" +
                "      connection:\n" +
                "      #{\n" +
                "        JsonModelConnection\n" +
                "        {\n" +
                "          class: org::dxl::Animal;\n" +
                "          url: 'my_url2';\n" +
                "        }\n" +
                "      }#\n" +
                "    }\n" +
                "    ingestMode: BitemporalDelta\n" +
                "    {\n" +
                "      mergeStrategy: DeleteIndicator\n" +
                "      {\n" +
                "        deleteField: deleted;\n" +
                "        deleteValues: ['Y', '1', 'true'];\n" +
                "      }\n" +
                "      transactionMilestoning: DateTime\n" +
                "      {\n" +
                "        dateTimeInName: 'inZ';\n" +
                "        dateTimeOutName: 'outZ';\n" +
                "      }\n" +
                "      validityMilestoning: DateTime\n" +
                "      {\n" +
                "        dateTimeFromName: 'FROM_Z';\n" +
                "        dateTimeThruName: 'THRU_Z';\n" +
                "        derivation: SourceSpecifiesFromAndThruDateTime\n" +
                "        {\n" +
                "          sourceDateTimeFromField: sourceFrom;\n" +
                "          sourceDateTimeThruField: sourceThru;\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "    targetShape: MultiFlat\n" +
                "    {\n" +
                "      modelClass: test::WrapperClass;\n" +
                "      transactionScope: ALL_TARGETS;\n" +
                "      parts:\n" +
                "      [\n" +
                "        {\n" +
                "          modelProperty: property1;\n" +
                "          targetName: 'TestDataset1';\n" +
                "          partitionFields: [propertyA, propertyB];\n" +
                "          deduplicationStrategy: AnyVersion;\n" +
                "        },\n" +
                "        {\n" +
                "          modelProperty: property2;\n" +
                "          targetName: 'TestDataset1';\n" +
                "          partitionFields: [propertyA, propertyB];\n" +
                "          deduplicationStrategy: DuplicateCount\n" +
                "          {\n" +
                "            duplicateCountName: 'duplicateCount';\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          modelProperty: property3;\n" +
                "          targetName: 'TestDataset2';\n" +
                "          deduplicationStrategy: None;\n" +
                "        }\n" +
                "      ];\n" +
                "    }\n" +
                "  }\n" +
                "  notifier:\n" +
                "  {\n" +
                "    notifyees:\n" +
                "    [\n" +
                "      Email\n" +
                "      {\n" +
                "        address: 'x.y@z.com';\n" +
                "      },\n" +
                "      PagerDuty\n" +
                "      {\n" +
                "        url: 'https://x.com';\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}\n");
    }
}