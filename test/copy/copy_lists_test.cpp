#include "common/type_utils.h"
#include "graph_test/graph_test.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class TinySnbListTest : public DBTest {

public:
    static bool CheckEquals(const std::vector<std::string>& expected, const Value& listVal) {
        if (listVal.dataType.typeID != VAR_LIST) {
            return false;
        }
        if (expected.size() != listVal.listVal.size()) {
            return false;
        }
        for (auto i = 0u; i < expected.size(); i++) {
            if (expected[i] != listVal.listVal[i]->toString()) {
                return false;
            }
        }
        return true;
    }
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

// Warning: This test assumes that each line in tinysnb's vPerson.csv gets
// the node offsets that start from 0 consecutively (so first line gets person ID 0, second person
// ID 1, so on and so forth).
TEST_F(TinySnbListTest, NodePropertyIntColumnWithList) {
    auto graph = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("person");
    auto& property = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "workedHours");
    auto col = graph->getNodesStore().getNodePropertyColumn(tableID, property.propertyID);
    ASSERT_TRUE(CheckEquals({"10", "5"}, col->readValueForTestingOnly(0)));
    ASSERT_TRUE(CheckEquals({"12", "8"}, col->readValueForTestingOnly(1)));
    ASSERT_TRUE(CheckEquals({"4", "5"}, col->readValueForTestingOnly(2)));
    ASSERT_TRUE(CheckEquals({"1", "9"}, col->readValueForTestingOnly(3)));
    ASSERT_TRUE(CheckEquals({"2"}, col->readValueForTestingOnly(4)));
    ASSERT_TRUE(CheckEquals({"3", "4", "5", "6", "7"}, col->readValueForTestingOnly(5)));
    ASSERT_TRUE(CheckEquals({"1"}, col->readValueForTestingOnly(6)));
    ASSERT_TRUE(
        CheckEquals({"10", "11", "12", "3", "4", "5", "6", "7"}, col->readValueForTestingOnly(7)));
}

TEST_F(TinySnbListTest, NodePropertyStringColumnWithList) {
    auto graph = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("person");
    auto& property = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "usedNames");
    auto col = graph->getNodesStore().getNodePropertyColumn(tableID, property.propertyID);
    ASSERT_TRUE(CheckEquals({"Aida"}, col->readValueForTestingOnly(0)));
    ASSERT_TRUE(CheckEquals({"Bobby"}, col->readValueForTestingOnly(1)));
    ASSERT_TRUE(CheckEquals({"Carmen", "Fred"}, col->readValueForTestingOnly(2)));
    ASSERT_TRUE(CheckEquals({"Wolfeschlegelstein", "Daniel"}, col->readValueForTestingOnly(3)));
    ASSERT_TRUE(CheckEquals({"Ein"}, col->readValueForTestingOnly(4)));
    ASSERT_TRUE(CheckEquals({"Fesdwe"}, col->readValueForTestingOnly(5)));
    ASSERT_TRUE(CheckEquals({"Grad"}, col->readValueForTestingOnly(6)));
    ASSERT_TRUE(CheckEquals({"Ad", "De", "Hi", "Kye", "Orlan"}, col->readValueForTestingOnly(7)));
}

TEST_F(TinySnbListTest, RelPropertyColumnWithList) {
    auto graph = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("studyAt");
    auto nodeTablesForAdjColumnAndProperties = catalog->getReadOnlyVersion()->getTableID("person");
    auto& property = catalog->getReadOnlyVersion()->getRelProperty(tableID, "places");
    auto col =
        graph->getRelsStore().getRelPropertyColumn(RelDirection::FWD, tableID, property.propertyID);
    ASSERT_TRUE(CheckEquals({"wwAewsdndweusd", "wek"}, col->readValueForTestingOnly(0)));
    ASSERT_TRUE(CheckEquals({"anew", "jsdnwusklklklwewsd"}, col->readValueForTestingOnly(1)));
    ASSERT_TRUE(
        CheckEquals({"awndsnjwejwen", "isuhuwennjnuhuhuwewe"}, col->readValueForTestingOnly(5)));
}
