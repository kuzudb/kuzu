#include "gtest/gtest.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/local_storage/local_hash_index.h"
#include "storage/overflow_file.h"

using namespace kuzu::common;
using namespace kuzu::storage;

bool isVisible(offset_t) {
    return true;
}

TEST(LocalHashIndexTests, LocalInserts) {
    BufferManager bm(":memory:", "", 256 * 1024 * 1024 /*bufferPoolSize*/,
        512 * 1024 * 1024 /*maxDBSize*/, nullptr, true);
    MemoryManager memoryManager(&bm, nullptr);
    auto overflowFile = std::make_unique<InMemOverflowFile>(memoryManager);
    auto* overflowFileHandle = overflowFile->addHandle();
    auto hashIndex =
        std::make_unique<LocalHashIndex>(memoryManager, PhysicalTypeID::INT64, overflowFileHandle);

    for (int64_t i = 0u; i < 100000; i++) {
        ASSERT_TRUE(hashIndex->insert(i, i * 2, isVisible));
    }
    for (int64_t i = 0u; i < 100000; i++) {
        ASSERT_FALSE(hashIndex->insert(i, i, isVisible));
    }

    for (int64_t i = 0u; i < 100000; i++) {
        hashIndex->delete_(i);
    }
    for (int64_t i = 0u; i < 100000; i++) {
        ASSERT_TRUE(hashIndex->insert(i, i, isVisible));
    }
}

std::string gen_random(const int len) {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";
    std::string tmp_s;
    tmp_s.reserve(len);

    for (int i = 0; i < len; ++i) {
        tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    return tmp_s;
}

TEST(LocalHashIndexTests, LocalStringInserts) {
    BufferManager bm(":memory:", "", 256 * 1024 * 1024 /*bufferPoolSize*/,
        512 * 1024 * 1024 /*maxDBSize*/, nullptr, true);
    MemoryManager memoryManager(&bm, nullptr);
    auto overflowFile = std::make_unique<InMemOverflowFile>(memoryManager);
    auto* overflowFileHandle = overflowFile->addHandle();
    auto hashIndex =
        std::make_unique<LocalHashIndex>(memoryManager, PhysicalTypeID::STRING, overflowFileHandle);

    std::vector<std::string> keys;
    for (int64_t i = 0u; i < 100; i++) {
        keys.push_back(gen_random(14));
        ASSERT_TRUE(hashIndex->insert(keys.back(), i * 2, isVisible));
    }
    for (int64_t i = 0u; i < 100; i++) {
        ASSERT_FALSE(hashIndex->insert(keys[i], i * 2, isVisible));
    }
}
