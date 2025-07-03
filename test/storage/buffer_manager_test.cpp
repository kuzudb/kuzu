#include <cstdint>

#include "common/constants.h"
#include "common/types/types.h"
#include "graph_test/graph_test.h"
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/buffer_manager/spiller.h"
#include "storage/enums/residency_state.h"
#include "storage/storage_manager.h"
#include "storage/table/chunked_node_group.h"
#include "storage/table/column_chunk.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace testing {

class BufferManagerTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
    void reserveAll() {
        auto* bm = getBufferManager(*database);
        // Can't use UINT64_MAX since it will overflow the usedMemory
        ASSERT_FALSE(bm->reserve(UINT64_MAX / 2));
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
        auto buffer = mm->allocateBuffer(false, 1024);
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

// Simulates the case where we try to evict a page during an optimistic read
TEST_F(BufferManagerTest, TestBMEvictionSlowRead) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    auto* fh = getClientContext(*conn)->getStorageManager()->getDataFH();

    auto* page = fh->pinPage(0, PageReadPolicy::READ_PAGE);
    *page = 112;
    fh->getPageState(0)->setDirty();
    fh->unpinPage(0);
    bool firstTry = true;
    // The mmap version will fail the first time, but will be run again since the page state was
    // changed during the read
#if BM_MALLOC
    fh->optimisticReadPage(0, [&](auto* frame) {
        ASSERT_TRUE(firstTry);
        firstTry = false;
        ASSERT_EQ(*frame, 112);
        // Should evict all evictable candidates in the eviction queue before failing
        // Should *not* evict the page we're currently reading
        reserveAll();
        // Probably will still succeed, but with ASAN (and BM_MALLOC) it should catch a read after
        // free if this frame was evicted
        ASSERT_EQ(*frame, 112);
    });
#else
    fh->optimisticReadPage(0, [&](auto* frame) {
        if (firstTry) {
            firstTry = false;
            ASSERT_EQ(*frame, 112);
            // Should evict all evictable candidates in the eviction queue before failing
            // Should evict the page we're currently reading
            reserveAll();
            // Frame was evicted and should now be zeroed
            // (not on macos, which does this lazily)
#ifndef __APPLE__
            ASSERT_EQ(*frame, 0);
#endif
        } else {
            // Second try it should be correct again
            ASSERT_EQ(*frame, 112);
        }
    });
#endif
}

} // namespace testing
} // namespace kuzu
