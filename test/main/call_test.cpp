#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::testing;

TEST_F(EmptyDBTest, SetThreadForExecution) {
    createDBAndConn();
    ASSERT_TRUE(conn->query("CALL threads=4")->isSuccess());
    ASSERT_EQ(conn->getMaxNumThreadForExec(), 4);
    ASSERT_TRUE(conn->query("CALL threads=2")->isSuccess());
    ASSERT_EQ(conn->getMaxNumThreadForExec(), 2);
    auto conn1 = std::make_unique<Connection>(database.get());
    ASSERT_EQ(conn1->getMaxNumThreadForExec(), std::thread::hardware_concurrency());
}

TEST_F(EmptyDBTest, SetTimeoutForExecution) {
    createDBAndConn();
    ASSERT_TRUE(conn->query("CALL timeout=40000")->isSuccess());
    ASSERT_EQ(conn->getQueryTimeOut(), 40000);
    ASSERT_TRUE(conn->query("CALL timeout=2000")->isSuccess());
    ASSERT_EQ(conn->getQueryTimeOut(), 2000);
    auto conn1 = std::make_unique<Connection>(database.get());
    ASSERT_EQ(conn1->getQueryTimeOut(), 0);
}
