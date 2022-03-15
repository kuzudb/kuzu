#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class TinySnbListTest : public InMemoryDBLoadedTest {

public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    static bool CheckEquals(const vector<string>& expected, const Literal& listVal) {
        if (listVal.dataType != LIST) {
            cout << "listVal data type is wrong" << endl;
            return false;
        }
        if (expected.size() != listVal.listVal.size()) {
            cout << "listVal size is wrong" << endl;
            return false;
        }
        for (auto i = 0u; i < expected.size(); i++) {
            if (expected[i] != listVal.listVal[i].toString()) {
                cout << "listVal element is not expected. " << expected[i]
                     << ", actually: " << listVal.listVal[i].toString() << endl;
                return false;
            }
        }
        return true;
    }
};

// Warning: This test assumes that each line in tinysnb's vPerson.csv gets
// the node offsets that start from 0 consecutively (so first line gets person ID 0, second person
// ID 1, so on and so forth).
TEST_F(TinySnbListTest, NodePropertyIntColumnWithList) {
    auto& catalog = defaultSystem->graph->getCatalog();
    auto label = catalog.getNodeLabelFromString("person");
    auto& property = catalog.getNodeProperty(label, "workedHours");
    auto col = defaultSystem->graph->getNodesStore().getNodePropertyColumn(label, property.id);
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
    auto& catalog = defaultSystem->graph->getCatalog();
    auto label = catalog.getNodeLabelFromString("person");
    auto& property = catalog.getNodeProperty(label, "usedNames");
    auto col = defaultSystem->graph->getNodesStore().getNodePropertyColumn(label, property.id);
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
    auto& catalog = defaultSystem->graph->getCatalog();
    auto relLabel = catalog.getRelLabelFromString("studyAt");
    auto nodeLabel = catalog.getNodeLabelFromString("person");
    auto& property = catalog.getRelProperty(relLabel, "places");
    auto col =
        defaultSystem->graph->getRelsStore().getRelPropertyColumn(relLabel, nodeLabel, property.id);
    ASSERT_TRUE(CheckEquals({"wwAewsdndweusd", "wek"}, col->readValue(0)));
    ASSERT_TRUE(CheckEquals({"anew", "jsdnwusklklklwewsd"}, col->readValue(1)));
    ASSERT_TRUE(CheckEquals({"awndsnjwejwen", "isuhuwennjnuhuhuwewe"}, col->readValue(5)));
}
