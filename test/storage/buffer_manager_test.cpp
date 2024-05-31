#include "graph_test/graph_test.h"
#include "gtest/gtest.h"
#include "spdlog/common.h"
#include "spdlog/spdlog.h"
#include "storage/buffer_manager/buffer_manager.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace testing {

class BufferManagerTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(BufferManagerTest, TestBMUsageForIdenticalQueries) {
    auto bm = getBufferManager(*database);
    auto initialMemoryUsage = bm->getUsedMemory();

    auto result = conn->query("MATCH (p:person) WHERE p.ID = 0 RETURN p.fName");
    ASSERT_TRUE(result->isSuccess()) << result->toString();
    auto memoryUsed = bm->getUsedMemory();
    result = conn->query("MATCH (p:person) WHERE p.ID = 0 RETURN p.fName");
    ASSERT_TRUE(result->isSuccess()) << result->toString();
    ASSERT_EQ(memoryUsed, bm->getUsedMemory())
        << "Memory usage after two identical queries should be identical";
    spdlog::info("Memory used initially: {}", initialMemoryUsage);
    spdlog::info("Memory used after transactions: {}", memoryUsed);
}

} // namespace testing
} // namespace kuzu
