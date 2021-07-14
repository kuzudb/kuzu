#include "gtest/gtest.h"
#include "test/storage/include/file_scanners/node_property_file_scanner.h"
#include "test/test_utility/include/test_helper.h"

#include "src/common/include/date.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class LoadDatesTest : public BaseGraphLoadingTestWithCatalog {

public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }
};

// Warning: This test assumes that each line in tinysnb's vPerson.csv gets
// the node offsets that start from 0 consecutively (so first line gets person ID 0, second person
// ID 1, so on and so forth).
TEST_F(LoadDatesTest, NodePropertyColumnWithDate) {
    StructuredNodePropertyFileScanner scanner(
        TEMP_TEST_DIR, catalog->getNodeLabelFromString("person"), "birthdate");
    EXPECT_EQ(Date::FromDate(1900, 1, 1).days, scanner.readProperty<int32_t>(0));
    EXPECT_EQ(Date::FromDate(1900, 1, 1).days, scanner.readProperty<int32_t>(1));
    EXPECT_EQ(Date::FromDate(1940, 6, 22).days, scanner.readProperty<int32_t>(2));
    EXPECT_EQ(Date::FromDate(1950, 7, 23).days, scanner.readProperty<int32_t>(3));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, scanner.readProperty<int32_t>(4));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, scanner.readProperty<int32_t>(5));
    EXPECT_EQ(Date::FromDate(1980, 10, 26).days, scanner.readProperty<int32_t>(6));
    EXPECT_EQ(Date::FromDate(1990, 11, 27).days, scanner.readProperty<int32_t>(7));
}
