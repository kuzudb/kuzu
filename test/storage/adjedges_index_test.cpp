#include <stdio.h>

#include <fstream>
#include <iostream>
#include <unordered_map>
#include <unordered_set>

#include "gtest/gtest.h"

#include "src/common/include/types.h"
#include "src/storage/include/adjedges_index.h"

using namespace graphflow::storage;
using namespace graphflow::common;
using namespace std;

class AdjEdgesIndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto f = ofstream("adjEdges1B4BTestFile", ios_base::out | ios_base::binary);
        uint8_t type = 0;
        uint32_t offset = 0;
        for (auto pageId = 0; pageId < 30; pageId++) {
            f.seekp(PAGE_SIZE * pageId);
            for (auto i = 0u; i < PAGE_SIZE / (sizeof(uint8_t) + sizeof(uint32_t)); i++) {
                f.write((char*)&type, sizeof(uint8_t));
                f.write((char*)&offset, sizeof(uint32_t));
                type++;
                if (UINT8_MAX == type) {
                    type = 0;
                }
                offset++;
            }
        }
        f.close();
    }

    void TearDown() override {
        auto fname = "col1B2BTestFile";
        remove(fname);
    }
};

// Tests the non-specialized Column template.
TEST_F(AdjEdgesIndexTest, GetVal) {
    auto numElements = 30 * (PAGE_SIZE / (sizeof(uint8_t) + sizeof(uint32_t)));
    BufferManager bufferManager(PAGE_SIZE * 30);
    auto col = new AdjEdges("adjEdges1B4BTestFile", numElements, 1 /*numBytesPerLabel*/,
        4 /*numBytesPerOffset*/, bufferManager);
    gfLabel_t fetchedLabel = 0;
    gfNodeOffset_t fetchedOffset = 0;
    auto expectedLabel = 0;
    for (auto offset = 0u; offset < numElements; offset++) {
        col->getVal(offset, fetchedLabel, fetchedOffset);
        ASSERT_EQ(fetchedOffset, offset);
        ASSERT_EQ(fetchedLabel, expectedLabel++);
        if (UINT8_MAX == expectedLabel) {
            expectedLabel = 0;
        }
    }
}
