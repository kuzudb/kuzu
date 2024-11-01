#include "gtest/gtest.h"
#include "storage/local_storage/local_hash_index.h"
#include "storage/storage_structure/overflow_file.h"

using namespace kuzu::common;
using namespace kuzu::storage;

bool isVisible(offset_t) {
    return true;
}

TEST(LocalHashIndexTests, LocalInserts) {
    DBFileIDAndName dbFileIDAndName{DBFileID{}, "in-mem-overflow"};
    auto overflowFile = std::make_unique<InMemOverflowFile>(dbFileIDAndName);
    PageCursor dummyCursor{0, 0};
    auto overflowFileHandle = std::make_unique<OverflowFileHandle>(*overflowFile, dummyCursor);
    auto hashIndex =
        std::make_unique<LocalHashIndex>(PhysicalTypeID::INT64, overflowFileHandle.get());

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
    DBFileIDAndName dbFileIDAndName{DBFileID{}, "in-mem-overflow"};
    auto overflowFile = std::make_unique<InMemOverflowFile>(dbFileIDAndName);
    PageCursor dummyCursor{INVALID_PAGE_IDX, 0};
    auto overflowFileHandle = std::make_unique<OverflowFileHandle>(*overflowFile, dummyCursor);
    auto hashIndex =
        std::make_unique<LocalHashIndex>(PhysicalTypeID::STRING, overflowFileHandle.get());

    std::vector<std::string> keys;
    for (int64_t i = 0u; i < 100; i++) {
        keys.push_back(gen_random(14));
        ASSERT_TRUE(hashIndex->insert(keys.back(), i * 2, isVisible));
    }
    for (int64_t i = 0u; i < 100; i++) {
        ASSERT_FALSE(hashIndex->insert(keys[i], i * 2, isVisible));
    }
}
