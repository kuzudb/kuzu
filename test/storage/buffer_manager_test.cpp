#include "common/constants.h"
#include "common/types/types.h"
#include "graph_test/graph_test.h"
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/buffer_manager/spiller.h"
#include "storage/enums/residency_state.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/column_chunk.h"

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

class EmptyBufferManagerTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/empty-db/");
    }
};

TEST_F(EmptyBufferManagerTest, TestSpillToDiskMemoryUsage) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    auto bm = getBufferManager(*database);
    auto mm = getMemoryManager(*database);
    auto initialUsedMemory = bm->getUsedMemory();
    {
        auto buffer = mm->mallocBuffer(false, 1024);
        ASSERT_EQ(initialUsedMemory + 1024, bm->getUsedMemory());
        ASSERT_NE(buffer->getBuffer().data(), nullptr);
    }
    ASSERT_EQ(initialUsedMemory, bm->getUsedMemory());

    {
        std::vector<std::unique_ptr<ColumnChunk>> chunks;
        chunks.push_back(std::make_unique<storage::ColumnChunk>(false,
            std::make_unique<ColumnChunkData>(*mm, LogicalType(LogicalTypeID::INT64), 1024, false,
                ResidencyState::IN_MEMORY, false)));
        auto chunkedNodeGroup = storage::ChunkedNodeGroup(std::move(chunks), 0);
        chunkedNodeGroup.setUnused(*mm);
        auto memoryWithChunks = bm->getUsedMemory();
        uint64_t memorySpilled = 0;
        bm->getSpillerOrSkip([&](auto& spiller) {
            spiller.addUnusedChunk(&chunkedNodeGroup);
            // Claim memory from unused chunks
            memorySpilled = spiller.claimNextGroup();
        });
        ASSERT_NE(memorySpilled, 0);
        // The chunks should be entirely on disk
        ASSERT_EQ(initialUsedMemory, bm->getUsedMemory() - memorySpilled);
        chunkedNodeGroup.loadFromDisk(*mm);
        // The chunks should be back in memory, but Spiller::claimNextGroup does not update
        // the amount of used memory itself, so we end up with the spilled memory recorded twice
        ASSERT_EQ(memoryWithChunks, bm->getUsedMemory() - memorySpilled);
    }
}

} // namespace testing
} // namespace kuzu
