#include "graph_test/graph_test.h"
#include "test_runner/test_runner.h"

using namespace kuzu::testing;

class GDSUtilsTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        systemConfig->maxNumThreads = 1;
        createDBAndConn();
        initGraph();
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/gds-rec-joins-small/");
    }
};

// This test tests that when there is a single worker thread in the system, which is set in the
// above SetUp function, then running GDS algorithms do not halt. GDS Algorithms use one worker
// thread to execute a FrontierTask, which acts as a "master thread" and makes calls to GDSUtils
// helper functions to parallelize actual computations. If there is a single worker thread in the
// system, this can halt the system. In order not to halt, GDSUtils functions instruct the
// TaskScheduler to spawn a new thread to make up for the "master thread". This test basically
// tests that we have a mechanism to make up for the "master thread" and the system does not halt.
// TODO(Guodong): FIX-ME. Fix on disk graph.
// TEST_F(GDSUtilsTest, OneWorkerThreadSystemDoesNotHalt) {
//     ASSERT_TRUE(conn->query("PROJECT GRAPH PK (person1, knows11) "
//                             "MATCH (a:person1) WHERE a.ID = 1 "
//                             "CALL single_sp_lengths(PK, a, 1,  true) "
//                             "RETURN *;")
//                     ->isSuccess());
// }
