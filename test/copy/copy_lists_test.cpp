#include "common/type_utils.h"
#include "graph_test/graph_test.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class TinySnbListTest : public DBTest {

public:
    static bool CheckEquals(const std::vector<std::string>& expected, const Value& listVal) {
        if (listVal.dataType.typeID != LIST) {
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
    ASSERT_TRUE(CheckEquals({"10", "5"}, col->readValue(0)));
    ASSERT_TRUE(CheckEquals({"12", "8"}, col->readValue(1)));
    ASSERT_TRUE(CheckEquals({"4", "5"}, col->readValue(2)));
    ASSERT_TRUE(CheckEquals({"1", "9"}, col->readValue(3)));
    ASSERT_TRUE(CheckEquals({"2"}, col->readValue(4)));
    ASSERT_TRUE(CheckEquals({"3", "4", "5", "6", "7"}, col->readValue(5)));
    ASSERT_TRUE(CheckEquals({"1"}, col->readValue(6)));
    ASSERT_TRUE(CheckEquals({"10", "11", "12", "3", "4", "5", "6", "7"}, col->readValue(7)));
}

TEST_F(TinySnbListTest, NodePropertyStringColumnWithList) {
    auto graph = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("person");
    auto& property = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "usedNames");
    auto col = graph->getNodesStore().getNodePropertyColumn(tableID, property.propertyID);
    ASSERT_TRUE(CheckEquals({"Aida"}, col->readValue(0)));
    ASSERT_TRUE(CheckEquals({"Bobby"}, col->readValue(1)));
    ASSERT_TRUE(CheckEquals({"Carmen", "Fred"}, col->readValue(2)));
    ASSERT_TRUE(CheckEquals({"Wolfeschlegelstein", "Daniel"}, col->readValue(3)));
    ASSERT_TRUE(CheckEquals({"Ein"}, col->readValue(4)));
    ASSERT_TRUE(CheckEquals({"Fesdwe"}, col->readValue(5)));
    ASSERT_TRUE(CheckEquals({"Grad"}, col->readValue(6)));
    ASSERT_TRUE(CheckEquals({"Ad", "De", "Hi", "Kye", "Orlan"}, col->readValue(7)));
}

TEST_F(TinySnbListTest, RelPropertyColumnWithList) {
    auto graph = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("studyAt");
    auto nodeTablesForAdjColumnAndProperties = catalog->getReadOnlyVersion()->getTableID("person");
    auto& property = catalog->getReadOnlyVersion()->getRelProperty(tableID, "places");
    auto col =
        graph->getRelsStore().getRelPropertyColumn(RelDirection::FWD, tableID, property.propertyID);
    ASSERT_TRUE(CheckEquals({"wwAewsdndweusd", "wek"}, col->readValue(0)));
    ASSERT_TRUE(CheckEquals({"anew", "jsdnwusklklklwewsd"}, col->readValue(1)));
    ASSERT_TRUE(CheckEquals({"awndsnjwejwen", "isuhuwennjnuhuhuwewe"}, col->readValue(5)));
}
