#include "test_helper/test_helper.h"

using namespace std;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class TinySnbCopyCSVDateTest : public InMemoryDBTest {
    string getInputCSVDir() override { return TestHelper::appendKuzuRootPath("dataset/tinysnb/"); }
};

// Warning: This test assumes that each line in tinysnb's vPerson.csv gets
// the node offsets that start from 0 consecutively (so first line gets person ID 0, second person
// ID 1, so on and so forth).
TEST_F(TinySnbCopyCSVDateTest, NodePropertyColumnWithDate) {
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("person");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "birthdate");
    auto storageManager = getStorageManager(*database);
    auto col =
        storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    auto dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    ASSERT_FALSE(col->isNull(0, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1900, 1, 1).days, col->readValue(0).val.dateVal.days);
    ASSERT_FALSE(col->isNull(1, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1900, 1, 1).days, col->readValue(1).val.dateVal.days);
    ASSERT_FALSE(col->isNull(2, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1940, 6, 22).days, col->readValue(2).val.dateVal.days);
    ASSERT_FALSE(col->isNull(3, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1950, 7, 23).days, col->readValue(3).val.dateVal.days);
    ASSERT_FALSE(col->isNull(4, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, col->readValue(4).val.dateVal.days);
    ASSERT_FALSE(col->isNull(5, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, col->readValue(5).val.dateVal.days);
    ASSERT_FALSE(col->isNull(6, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, col->readValue(6).val.dateVal.days);
    ASSERT_FALSE(col->isNull(7, dummyReadOnlyTrx.get()));
    EXPECT_EQ(Date::FromDate(1990, 11, 27).days, col->readValue(7).val.dateVal.days);
}
