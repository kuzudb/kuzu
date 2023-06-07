#include "graph_test/graph_test.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::testing;
using namespace kuzu::transaction;

namespace kuzu {
namespace testing {

class CopyLargeNpyTest : public DBTest {
public:
    std::string getInputDir() { return TestHelper::appendKuzuRootPath("dataset/npy-20k/"); }
};

TEST_F(CopyLargeNpyTest, CopyLargeNpyTest) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("npytable");
    auto property = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "id");
    auto col = storageManager->getNodesStore().getNodePropertyColumn(tableID, property.propertyID);
    for (size_t i = 0; i < 20000; ++i) {
        ASSERT_EQ(col->readValueForTestingOnly(i).val.int64Val, i);
    }
    property = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "f32");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, property.propertyID);
    for (auto rowIdx = 0u; rowIdx < 20000; rowIdx++) {
        auto row = col->readValueForTestingOnly(rowIdx);
        for (auto colIdx = 0u; colIdx < 10; colIdx++) {
            if (row.nestedTypeVal[colIdx]->val.floatVal != (float)(rowIdx * 10 + colIdx)) {
                std::cout << "rowIdx: " << rowIdx << " colIdx: " << colIdx << std::endl;
            }
            ASSERT_EQ(row.nestedTypeVal[colIdx]->val.floatVal, (float)(rowIdx * 10 + colIdx));
        }
    }
    for (size_t i = 0; i < 200000; ++i) {
        size_t rowIdx = i / 10;
        size_t colIdx = i % 10;
        if (col->readValueForTestingOnly(rowIdx).nestedTypeVal[colIdx]->val.floatVal != (float)i) {
            std::cout << "rowIdx: " << rowIdx << " colIdx: " << colIdx << std::endl;
        }
        ASSERT_EQ(
            col->readValueForTestingOnly(rowIdx).nestedTypeVal[colIdx]->val.floatVal, (float)i);
    }
}

} // namespace testing
} // namespace kuzu
