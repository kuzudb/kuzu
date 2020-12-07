#include <stdio.h>

#include <fstream>
#include <iostream>
#include <unordered_map>
#include <unordered_set>

#include "gtest/gtest.h"

#include "src/common/include/types.h"
#include "src/storage/include/structures/column.h"

using namespace graphflow::storage;
using namespace graphflow::common;
using namespace std;

class ColumnIntegerTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto f = ofstream("colIntTestFile", ios_base::out | ios_base::binary);
        uint32_t val = 0;
        for (auto pageId = 0; pageId < 30; pageId++) {
            for (auto i = 0u; i < PAGE_SIZE / sizeof(int32_t); i++) {
                f.write((char*)&val, sizeof(int32_t));
                val++;
            }
        }
        f.close();
    }

    void TearDown() override {
        auto fname = "colIntTestFile";
        remove(fname);
    }
};

// Tests the PropertyColumn template with T=gfInt_t.
TEST_F(ColumnIntegerTest, GetVal) {
    auto numElements = 30 * (PAGE_SIZE / sizeof(int32_t));
    BufferManager bufferManager(PAGE_SIZE * 30);
    auto col = new PropertyColumnInt("colIntTestFile", numElements, bufferManager);
    int32_t fetched = 0;
    for (auto offset = 0u; offset < numElements; offset++) {
        col->getVal(offset, fetched);
        ASSERT_EQ(fetched, offset);
    }
}
