#include "gtest/gtest.h"
#include "test/storage/include/node_property_file_scanner.h"
#include "test/test_utility/include/test_helper.h"

#include "src/common/include/date.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class LoadDatesTest : public BaseGraphLoadingTest {
public:
    const string DATA_SET_DIR = "dataset/string-property-test/";
    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        auto bufferManager = make_unique<BufferManager>(DEFAULT_BUFFER_POOL_SIZE);
        graph = make_unique<Graph>(TEMP_TEST_DIR, *bufferManager);
    }
    string getInputCSVDir() override { return "dataset/tinysnb/"; }

public:
    unique_ptr<Graph> graph;
};

// Warning: This test assumes that each line in tinysnb's vPerson.csv gets
// the node offsets that start from 0 consecutively (so first line gets person ID 0, second person
// ID 1, so on and so forth).
TEST_F(LoadDatesTest, NodePropertyColumnWithDate) {
    StructuredNodePropertyFileScanner scanner(
        TEMP_TEST_DIR, graph->getCatalog().getNodeLabelFromString("person"), "birthdate");
    EXPECT_EQ(Date::FromDate(1900, 1, 1).days, scanner.readProperty<int32_t>(0));
    EXPECT_EQ(Date::FromDate(1900, 1, 1).days, scanner.readProperty<int32_t>(1));
    EXPECT_EQ(Date::FromDate(1940, 6, 22).days, scanner.readProperty<int32_t>(2));
    EXPECT_EQ(Date::FromDate(1950, 7, 23).days, scanner.readProperty<int32_t>(3));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, scanner.readProperty<int32_t>(4));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, scanner.readProperty<int32_t>(5));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, scanner.readProperty<int32_t>(6));
    EXPECT_EQ(Date::FromDate(1990, 11, 27).days, scanner.readProperty<int32_t>(7));
}
