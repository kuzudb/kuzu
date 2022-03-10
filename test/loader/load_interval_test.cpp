#include <string>

#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class TinySnbIntervalTest : public InMemoryDBLoadedTest {

public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }
};

// Warning: This test assumes that each line in tinysnb's vPerson.csv gets
// the node offsets that start from 0 consecutively (so first line gets person ID 0, second person
// ID 1, so on and so forth).
TEST_F(TinySnbIntervalTest, NodePropertyColumnWithInterval) {
    auto& catalog = defaultSystem->graph->getCatalog();
    auto label = catalog.getNodeLabelFromString("person");
    auto propertyIdx = catalog.getNodeProperty(label, "lastJobDuration");
    auto col = defaultSystem->graph->getNodesStore().getNodePropertyColumn(label, propertyIdx.id);
    EXPECT_EQ(Interval::FromCString(
                  "3 years 2 days 13 hours 2 minutes", strlen("3 years 2 days 13 hours 2 minutes")),
        col->readValue(0).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "10 years 5 months 13 hours 24 us", strlen("10 years 5 months 13 hours 24 us")),
        col->readValue(1).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "48 hours 24 minutes 11 seconds", strlen("48 hours 24 minutes 11 seconds")),
        col->readValue(2).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "10 years 5 months 13 hours 24 us", strlen("10 years 5 months 13 hours 24 us")),
        col->readValue(3).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "48 hours 24 minutes 11 seconds", strlen("48 hours 24 minutes 11 seconds")),
        col->readValue(4).val.intervalVal);
    EXPECT_EQ(
        Interval::FromCString("18 minutes 24 milliseconds", strlen("18 minutes 24 milliseconds")),
        col->readValue(5).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "10 years 5 months 13 hours 24 us", strlen("10 years 5 months 13 hours 24 us")),
        col->readValue(6).val.intervalVal);
    EXPECT_EQ(Interval::FromCString(
                  "3 years 2 days 13 hours 2 minutes", strlen("3 years 2 days 13 hours 2 minutes")),
        col->readValue(7).val.intervalVal);
}
