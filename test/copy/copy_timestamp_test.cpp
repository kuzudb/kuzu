#include "graph_test/graph_test.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class TinySnbTimestampTest : public DBTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

// Warning: This test assumes that each line in tinysnb's vPerson.csv gets
// the node offsets that start from 0 consecutively (so first line gets person ID 0, second person
// ID 1, so on and so forth).
TEST_F(TinySnbTimestampTest, NodePropertyColumnWithTimestamp) {
    auto graph = getStorageManager(*database);
    auto& catalog = *getCatalog(*database);
    auto table = catalog.getReadOnlyVersion()->getTableID("person");
    auto propertyIdx = catalog.getReadOnlyVersion()->getNodeProperty(table, "registerTime");
    auto col = graph->getNodesStore().getNodePropertyColumn(table, propertyIdx.propertyID);
    EXPECT_EQ(Timestamp::FromDatetime(Date::FromDate(2011, 8, 20), Time::FromTime(11, 25, 30)),
        col->readValueForTestingOnly(0).val.timestampVal);
    EXPECT_EQ(
        Timestamp::FromDatetime(Date::FromDate(2008, 11, 3), Time::FromTime(15, 25, 30, 526)).value,
        col->readValueForTestingOnly(1).val.timestampVal.value);
    EXPECT_EQ(Timestamp::FromDatetime(Date::FromDate(1911, 8, 20), Time::FromTime(2, 32, 21)),
        col->readValueForTestingOnly(2).val.timestampVal);
    EXPECT_EQ(Timestamp::FromDatetime(Date::FromDate(2031, 11, 30), Time::FromTime(12, 25, 30)),
        col->readValueForTestingOnly(3).val.timestampVal);
    EXPECT_EQ(Timestamp::FromDatetime(Date::FromDate(1976, 12, 23), Time::FromTime(11, 21, 42)),
        col->readValueForTestingOnly(4).val.timestampVal);
    EXPECT_EQ(
        Timestamp::FromDatetime(Date::FromDate(1972, 7, 31), Time::FromTime(13, 22, 30, 678559)),
        col->readValueForTestingOnly(5).val.timestampVal);
    EXPECT_EQ(Timestamp::FromDatetime(Date::FromDate(1976, 12, 23), Time::FromTime(04, 41, 42)),
        col->readValueForTestingOnly(6).val.timestampVal);
    EXPECT_EQ(Timestamp::FromDatetime(Date::FromDate(2023, 2, 21), Time::FromTime(13, 25, 30)),
        col->readValueForTestingOnly(7).val.timestampVal);
}
