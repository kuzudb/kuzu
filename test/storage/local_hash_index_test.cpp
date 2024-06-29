#include "gtest/gtest.h"
#include "storage/local_storage/local_hash_index.h"
#include "storage/storage_structure/overflow_file.h"

using namespace kuzu::common;
using namespace kuzu::storage;

TEST(LocalHashIndexTests, LocalInserts) {
    DBFileIDAndName dbFileIDAndName{DBFileID{DBFileType::NODE_INDEX}, "in-mem-overflow"};
    auto overflowFile = std::make_unique<InMemOverflowFile>(dbFileIDAndName);
    PageCursor dummyCursor{0, 0};
    auto overflowFileHandle = std::make_unique<OverflowFileHandle>(*overflowFile, dummyCursor);
    auto hashIndex =
        std::make_unique<LocalHashIndex>(PhysicalTypeID::INT64, overflowFileHandle.get());

    for (int64_t i = 0u; i < 100; i++) {
        ASSERT_TRUE(hashIndex->insert(i, i * 2));
    }
    for (int64_t i = 0u; i < 100; i++) {
        ASSERT_FALSE(hashIndex->insert(i, i));
    }

    for (int64_t i = 0u; i < 100; i++) {
        hashIndex->delete_(i);
    }
    for (int64_t i = 0u; i < 100; i++) {
        ASSERT_TRUE(hashIndex->insert(i, i));
    }
}
