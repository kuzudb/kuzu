#include <string>

#include "graph_test/graph_test.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class TinySnbCopyIntervalTest : public DBTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

// Warning: This test assumes that each line in tinysnb's vPerson.csv gets
// the node offsets that start from 0 consecutively (so first line gets person ID 0, second person
// ID 1, so on and so forth).
TEST_F(TinySnbCopyIntervalTest, NodePropertyColumnWithInterval) {
    auto graph = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("person");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "lastJobDuration");
    auto col = graph->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    EXPECT_EQ(Interval::FromCString(
                  "3 years 2 days 13 hours 2 minutes", strlen("3 years 2 days 13 hours 2 minutes")),
        col->readValueForTestingOnly(0).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "10 years 5 months 13 hours 24 us", strlen("10 years 5 months 13 hours 24 us")),
        col->readValueForTestingOnly(1).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "48 hours 24 minutes 11 seconds", strlen("48 hours 24 minutes 11 seconds")),
        col->readValueForTestingOnly(2).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "10 years 5 months 13 hours 24 us", strlen("10 years 5 months 13 hours 24 us")),
        col->readValueForTestingOnly(3).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "48 hours 24 minutes 11 seconds", strlen("48 hours 24 minutes 11 seconds")),
        col->readValueForTestingOnly(4).val.intervalVal);
    EXPECT_EQ(
        Interval::FromCString("18 minutes 24 milliseconds", strlen("18 minutes 24 milliseconds")),
        col->readValueForTestingOnly(5).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "10 years 5 months 13 hours 24 us", strlen("10 years 5 months 13 hours 24 us")),
        col->readValueForTestingOnly(6).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "3 years 2 days 13 hours 2 minutes", strlen("3 years 2 days 13 hours 2 minutes")),
        col->readValueForTestingOnly(7).val.intervalVal);
}
