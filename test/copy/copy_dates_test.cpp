#include "graph_test/graph_test.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;
using namespace kuzu::transaction;

class TinySnbCopyDateTest : public DBTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

// Warning: This test assumes that each line in tinysnb's vPerson.csv gets
// the node offsets that start from 0 consecutively (so first line gets person ID 0, second person
// ID 1, so on and so forth).
TEST_F(TinySnbCopyDateTest, NodePropertyColumnWithDate) {
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("person");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "birthdate");
    auto storageManager = getStorageManager(*database);
    auto col =
        storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    auto dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    ASSERT_FALSE(col->isNull(0, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1900, 1, 1).days, col->readValueForTestingOnly(0).val.dateVal.days);
    ASSERT_FALSE(col->isNull(1, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1900, 1, 1).days, col->readValueForTestingOnly(1).val.dateVal.days);
    ASSERT_FALSE(col->isNull(2, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1940, 6, 22).days, col->readValueForTestingOnly(2).val.dateVal.days);
    ASSERT_FALSE(col->isNull(3, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1950, 7, 23).days, col->readValueForTestingOnly(3).val.dateVal.days);
    ASSERT_FALSE(col->isNull(4, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, col->readValueForTestingOnly(4).val.dateVal.days);
    ASSERT_FALSE(col->isNull(5, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, col->readValueForTestingOnly(5).val.dateVal.days);
    ASSERT_FALSE(col->isNull(6, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, col->readValueForTestingOnly(6).val.dateVal.days);
    ASSERT_FALSE(col->isNull(7, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1990, 11, 27).days, col->readValueForTestingOnly(7).val.dateVal.days);
}
