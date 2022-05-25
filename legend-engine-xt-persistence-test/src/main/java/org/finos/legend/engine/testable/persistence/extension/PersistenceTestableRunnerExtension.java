package org.finos.legend.engine.testable.persistence.extension;

import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.protocol.pure.PureClientVersions;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.v1.model.test.AtomicTestId;
import org.finos.legend.engine.protocol.pure.v1.model.test.result.TestResult;
import org.finos.legend.engine.testable.extension.TestRunner;
import org.finos.legend.engine.testable.extension.TestableRunnerExtension;
import org.finos.legend.pure.generated.Root_meta_pure_persistence_metamodel_Persistence;
import org.finos.legend.pure.generated.Root_meta_pure_test_TestSuite;
import org.finos.legend.pure.generated.Root_meta_pure_test_Testable;

import java.util.List;

public class PersistenceTestableRunnerExtension implements TestableRunnerExtension
{
    private String pureVersion = PureClientVersions.production;

    @Override
    public TestRunner getTestRunner(Root_meta_pure_test_Testable testable)
    {
        if (testable instanceof Root_meta_pure_persistence_metamodel_Persistence)
        {
            return new PersistenceTestRunner((Root_meta_pure_persistence_metamodel_Persistence) testable, pureVersion);
        }
        return null;
    }

    public List<TestResult> executeAllTest(Root_meta_pure_test_Testable testable, PureModel pureModel, PureModelContextData pureModelContextData)
    {
        if (!(testable instanceof Root_meta_pure_persistence_metamodel_Persistence))
        {
            throw new UnsupportedOperationException("Expected Persistence testable. Found : " + testable.getName());
        }

        PersistenceTestRunner testRunner = new PersistenceTestRunner((Root_meta_pure_persistence_metamodel_Persistence) testable, pureVersion);

//        return ((Root_meta_pure_persistence_metamodel_Persistence) testable)._tests().flatCollect(testSuite -> {
//            List<AtomicTestId> atomicTestIds = ((Root_meta_pure_test_TestSuite) testSuite)._tests().collect(test -> {
//                AtomicTestId id = new AtomicTestId();
//                id.testSuiteId = testSuite._id();
//                id.atomicTestId = test._id();
//                return id;
//            }).toList();
//            return testRunner.executeTestSuite((Root_meta_pure_test_TestSuite) testSuite, atomicTestIds, pureModel, pureModelContextData);
//        }).toList();
        return null;

    }

    public void setPureVersion(String pureVersion)
    {
        this.pureVersion = pureVersion;
    }
}
