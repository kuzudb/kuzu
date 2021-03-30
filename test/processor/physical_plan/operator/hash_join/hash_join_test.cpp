#include "data_chunks_mock_operator.h"
#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"
#include "src/processor/include/physical_plan/physical_plan.h"
#include "src/processor/include/processor.h"

TEST(HashJoinTests, HashJoinTest1T1) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(0, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(0, 0, 0, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(1);
    auto result = processor->execute(move(plan), 1);
    ASSERT_EQ(result->numTuples, 4000);
}

TEST(HashJoinTests, HashJoinTest1T2) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(0, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(0, 0, 0, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(2);
    auto result = processor->execute(move(plan), 2);
    ASSERT_EQ(result->numTuples, 4000);
}

TEST(HashJoinTests, HashJoinTest1T4) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(0, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(0, 0, 0, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(4);
    auto result = processor->execute(move(plan), 4);
    ASSERT_EQ(result->numTuples, 4000);
}

TEST(HashJoinTests, HashJoinTest1T8) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(0, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(0, 0, 0, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(8);
    auto result = processor->execute(move(plan), 8);
    ASSERT_EQ(result->numTuples, 4000);
}

TEST(HashJoinTests, HashJoinTest2T1) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(1, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(1, 0, 1, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(1);
    auto result = processor->execute(move(plan), 1);
    ASSERT_EQ(result->numTuples, 400);
}

TEST(HashJoinTests, HashJoinTest2T2) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(1, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(1, 0, 1, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(2);
    auto result = processor->execute(move(plan), 2);
    ASSERT_EQ(result->numTuples, 400);
}

TEST(HashJoinTests, HashJoinTest2T4) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(1, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(1, 0, 1, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(4);
    auto result = processor->execute(move(plan), 4);
    ASSERT_EQ(result->numTuples, 400);
}

TEST(HashJoinTests, HashJoinTest2T8) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(1, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(1, 0, 1, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(8);
    auto result = processor->execute(move(plan), 8);
    ASSERT_EQ(result->numTuples, 400);
}

TEST(HashJoinTests, HashJoinTest3T1) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(2, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(2, 0, 2, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(1);
    auto result = processor->execute(move(plan), 1);
    ASSERT_EQ(result->numTuples, 400);
}

TEST(HashJoinTests, HashJoinTest3T2) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(2, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(2, 0, 2, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(2);
    auto result = processor->execute(move(plan), 2);
    ASSERT_EQ(result->numTuples, 400);
}

TEST(HashJoinTests, HashJoinTest3T4) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(2, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(2, 0, 2, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(4);
    auto result = processor->execute(move(plan), 4);
    ASSERT_EQ(result->numTuples, 400);
}

TEST(HashJoinTests, HashJoinTest3T8) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(2, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(2, 0, 2, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(8);
    auto result = processor->execute(move(plan), 8);
    ASSERT_EQ(result->numTuples, 400);
}

TEST(HashJoinTests, HashJoinTest4T1) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(1, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(1, 0, 2, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(1);
    auto result = processor->execute(move(plan), 1);
    ASSERT_EQ(result->numTuples, 0);
}

TEST(HashJoinTests, HashJoinTest4T4) {
    MorselDesc buildMorsel(10);
    MorselDesc probeMorsel(4);
    auto buildOp = make_unique<ScanMockOp>(buildMorsel);
    auto probeOp = make_unique<ProbeScanMockOp>(probeMorsel);

    auto sharedState = make_shared<HashJoinSharedState>();
    auto hashJoinBuild = make_unique<HashJoinBuild>(1, 0, move(buildOp));
    sharedState->hashTable = hashJoinBuild->initializeHashTable();
    hashJoinBuild->sharedState = sharedState;
    auto hashJoinProbe =
        make_unique<HashJoinProbe<false>>(1, 0, 2, 0, move(hashJoinBuild), move(probeOp));
    hashJoinProbe->sharedState = sharedState;
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(hashJoinProbe)));
    auto processor = make_unique<QueryProcessor>(4);
    auto result = processor->execute(move(plan), 4);
    ASSERT_EQ(result->numTuples, 0);
}
