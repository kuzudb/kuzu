#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

#include "src/common/include/date.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class TinySnbDateTest : public InMemoryDBLoadedTest {

public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }
};

// Warning: This test assumes that each line in tinysnb's vPerson.csv gets
// the node offsets that start from 0 consecutively (so first line gets person ID 0, second person
// ID 1, so on and so forth).
TEST_F(TinySnbDateTest, NodePropertyColumnWithDate) {
    auto& catalog = defaultSystem->graph->getCatalog();
    auto label = catalog.getNodeLabelFromString("person");
    auto propertyIdx = catalog.getNodeProperty(label, "birthdate");
    auto col = defaultSystem->graph->getNodesStore().getNodePropertyColumn(label, propertyIdx.id);
    NumericMetric numBufferHits(true), numBufferMisses(true), numIO(true);
    BufferManagerMetrics metrics(numBufferHits, numBufferMisses, numIO);
    EXPECT_EQ(Date::FromDate(1900, 1, 1).days, col->readValue(0, metrics).val.dateVal.days);
    EXPECT_EQ(Date::FromDate(1900, 1, 1).days, col->readValue(1, metrics).val.dateVal.days);
    EXPECT_EQ(Date::FromDate(1940, 6, 22).days, col->readValue(2, metrics).val.dateVal.days);
    EXPECT_EQ(Date::FromDate(1950, 7, 23).days, col->readValue(3, metrics).val.dateVal.days);
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, col->readValue(4, metrics).val.dateVal.days);
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, col->readValue(5, metrics).val.dateVal.days);
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, col->readValue(6, metrics).val.dateVal.days);
    EXPECT_EQ(Date::FromDate(1990, 11, 27).days, col->readValue(7, metrics).val.dateVal.days);
}
