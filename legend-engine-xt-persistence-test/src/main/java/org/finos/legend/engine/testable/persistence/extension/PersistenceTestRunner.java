package org.finos.legend.engine.testable.persistence.extension;


import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.plan.execution.PlanExecutor;
import org.finos.legend.engine.plan.execution.result.ConstantResult;
import org.finos.legend.engine.plan.execution.result.Result;
import org.finos.legend.engine.plan.execution.result.StreamingResult;
import org.finos.legend.engine.plan.execution.result.serialization.SerializationFormat;
import org.finos.legend.engine.plan.generation.extension.PlanGeneratorExtension;
import org.finos.legend.engine.plan.generation.transformers.PlanTransformer;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.SingleExecutionPlan;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.ParameterValue;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.Persistence;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.PersistenceTest;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.persistence.PersistenceTestSuite;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.service.PureSingleExecution;
import org.finos.legend.engine.protocol.pure.v1.model.test.AtomicTestId;
import org.finos.legend.engine.protocol.pure.v1.model.test.Test;
import org.finos.legend.engine.protocol.pure.v1.model.test.assertion.TestAssertion;
import org.finos.legend.engine.protocol.pure.v1.model.test.assertion.status.AssertFail;
import org.finos.legend.engine.protocol.pure.v1.model.test.assertion.status.AssertionStatus;
import org.finos.legend.engine.protocol.pure.v1.model.test.result.TestError;
import org.finos.legend.engine.protocol.pure.v1.model.test.result.TestFailed;
import org.finos.legend.engine.protocol.pure.v1.model.test.result.TestPassed;
import org.finos.legend.engine.protocol.pure.v1.model.test.result.TestResult;
import org.finos.legend.engine.testable.extension.TestRunner;
import org.finos.legend.pure.generated.Root_meta_pure_persistence_metamodel_Persistence;
import org.finos.legend.pure.generated.Root_meta_pure_router_extension_RouterExtension;
import org.finos.legend.pure.generated.Root_meta_pure_test_AtomicTest;
import org.finos.legend.pure.generated.Root_meta_pure_test_TestSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static org.finos.legend.engine.language.pure.compiler.toPureGraph.HelperModelBuilder.getElementFullPath;

public class PersistenceTestRunner implements TestRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceTestRunner.class);

    private Root_meta_pure_persistence_metamodel_Persistence purePersistence;

    private MutableList<PlanGeneratorExtension> extensions;
    private PlanExecutor planExecutor;

    private String pureVersion;

    public PersistenceTestRunner(Root_meta_pure_persistence_metamodel_Persistence purePersistence, String pureVersion) {
        this.purePersistence = purePersistence;
        this.planExecutor = PlanExecutor.newPlanExecutorWithAvailableStoreExecutors();
        this.extensions = Lists.mutable.withAll(ServiceLoader.load(PlanGeneratorExtension.class));
        this.pureVersion = pureVersion;
    }

    @Override
    public TestResult executeAtomicTest(Root_meta_pure_test_AtomicTest atomicTest, PureModel pureModel, PureModelContextData data) {
        throw new UnsupportedOperationException("Service Test should be executed in context of Service Test Suite only");
    }

    @Override
    public List<TestResult> executeTestSuite(Root_meta_pure_test_TestSuite testSuite, List<AtomicTestId> atomicTestIds, PureModel pureModel, PureModelContextData data) {
        RichIterable<? extends Root_meta_pure_router_extension_RouterExtension> routerExtensions = extensions.flatCollect(e -> e.getExtraRouterExtensions(pureModel));
        MutableList<PlanTransformer> planTransformers = extensions.flatCollect(PlanGeneratorExtension::getExtraPlanTransformers);

        Persistence persistence = ListIterate.detect(data.getElementsOfType(Persistence.class), ele -> ele.getPath().equals(getElementFullPath(purePersistence, pureModel.getExecutionSupport())));
        PersistenceTestSuite suite = ListIterate.detect(persistence.testSuites, ts -> ts.id.equals(testSuite._id()));
        List<String> testIds = ListIterate.collect(atomicTestIds, testId -> testId.atomicTestId);
        //return executeSingleExecutionTestSuite((PureSingleExecution) service.execution, suite, testIds, pureModel, data, routerExtensions, planTransformers);
        return null;
    }

    private List<org.finos.legend.engine.protocol.pure.v1.model.test.result.TestResult> executeSingleExecutionTestSuite(PureSingleExecution execution, PersistenceTestSuite suite, List<String> testIds, PureModel pureModel, PureModelContextData data, RichIterable<? extends Root_meta_pure_router_extension_RouterExtension> routerExtensions, MutableList<PlanTransformer> planTransformers) {
        List<org.finos.legend.engine.protocol.pure.v1.model.test.result.TestResult> results = Lists.mutable.empty();
        Pair<Runtime, List<Closeable>> runtimeWithCloseables = null;

        try {
//            runtimeWithCloseables = getTestRuntimeAndClosableResources(execution.runtime, suite.testData, data);
//            Runtime testSuiteRuntime = runtimeWithCloseables.getOne();
//            PureSingleExecution testPureSingleExecution = shallowCopySingleExecution(execution);
//            testPureSingleExecution.runtime = testSuiteRuntime;
////
//            ExecutionPlan executionPlan = ServicePlanGenerator.generateExecutionPlan(testPureSingleExecution, null, pureModel, pureVersion, PlanPlatform.JAVA, null, routerExtensions, planTransformers);
//            SingleExecutionPlan singleExecutionPlan = (SingleExecutionPlan) executionPlan;
//            JavaHelper.compilePlan(singleExecutionPlan, null);

            for (Test test : suite.tests) {
                if (testIds.contains(test.id)) {
//                    org.finos.legend.engine.protocol.pure.v1.model.test.result.TestResult testResult = executePersistenceTest((PersistenceTest) test, singleExecutionPlan);
//                    testResult.testable = getElementFullPath(pureService, pureModel.getExecutionSupport());
//                    testResult.atomicTestId.testSuiteId = suite.id;
//
//                    results.add(testResult);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred executing service test suites.\n" + e);
        } finally {
            if (runtimeWithCloseables != null) {
                runtimeWithCloseables.getTwo().forEach(closeable -> {
                    try {
                        closeable.close();
                    } catch (IOException e) {
                        LOGGER.warn("Exception occurred closing closeable resource" + e);
                    }
                });
            }
        }

        return results;
    }

    private org.finos.legend.engine.protocol.pure.v1.model.test.result.TestResult executePersistenceTest(PersistenceTest PersistenceTest, SingleExecutionPlan executionPlan) {
        AtomicTestId atomicTestId = new AtomicTestId();
        atomicTestId.atomicTestId = PersistenceTest.id;

        try {
            Map<String, Object> parameters = Maps.mutable.empty();
            if (PersistenceTest.parameters != null) {
                for (ParameterValue parameterValue : PersistenceTest.parameters) {
                    //parameters.put(parameterValue.name, parameterValue.value.accept(new PrimitiveValueSpecificationToObjectVisitor()));
                }
            }

            Result result = this.planExecutor.execute(executionPlan, parameters);

            boolean isResultReusable = executionPlan.rootExecutionNode.isResultPrimitiveType();
            if (isResultReusable && result instanceof StreamingResult) {
                result = new ConstantResult(((StreamingResult) result).flush(((StreamingResult) result).getSerializer(SerializationFormat.RAW)));
            }

            org.finos.legend.engine.protocol.pure.v1.model.test.result.TestResult testResult;

            List<AssertionStatus> assertionStatusList = Lists.mutable.empty();
            for (TestAssertion assertion : PersistenceTest.assertions) {
                //assertionStatusList.add(assertion.accept(new PersistenceTestAssertionEvaluator(result)));
                if (!isResultReusable) {
                    result = this.planExecutor.execute(executionPlan, parameters);
                }
            }

            List<AssertFail> failedAsserts = ListIterate.selectInstancesOf(assertionStatusList, AssertFail.class);

            if (failedAsserts.isEmpty()) {
                testResult = new TestPassed();
                testResult.atomicTestId = atomicTestId;
            } else {
                TestFailed testFailed = new TestFailed();
                testFailed.assertStatuses = assertionStatusList;

                testResult = testFailed;
                testResult.atomicTestId = atomicTestId;
            }

            return testResult;
        } catch (Exception e) {
            TestError testError = new TestError();
            testError.atomicTestId = atomicTestId;
            testError.error = e.toString();

            return testError;
        }
    }
}
