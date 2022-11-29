#include "test/test_utility/include/test_helper.h"

using namespace std;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class CopyCSVArrowNodeTest : public InMemoryDBTest {
    string getInputCSVDir() override { return "dataset/copy-csv-arrow-node-test/"; }

public:
    static bool CheckEquals(const vector<string>& expected, const Literal& listVal) {
        if (listVal.dataType.typeID != LIST) {
            return false;
        }
        if (expected.size() != listVal.listVal.size()) {
            return false;
        }
        for (auto i = 0u; i < expected.size(); i++) {
            if (expected[i] != TypeUtils::toString(listVal.listVal[i])) {
                cout << expected[i] << " vs " << TypeUtils::toString(listVal.listVal[i]) << endl;
                return false;
            }
        }
        return true;
    }
};

TEST_F(CopyCSVArrowNodeTest, ArrowTest) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("arrow");

    // Test for random_100k_rows.csv
    //1829-10-28 17:13:37

    for (int i = 0; i <= 8; i++) {
        string name;
        if (i == 0) {
            name = "id";
        } else {
            name = "feature" + to_string(i);
        }
        auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(
                                        tableID, name);
        auto col =
                storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
        if (i == 0) {
            EXPECT_EQ(0, col->readValue(0).val.int64Val);
        } else if (i == 1) {
            EXPECT_EQ(73, col->readValue(0).val.int64Val);
        } else if (i == 2) {
            EXPECT_EQ(3.2585065282054626, col->readValue(0).val.doubleVal);
        } else if (i == 4) {
            EXPECT_EQ(Date::FromDate(1829, 10, 28), col->readValue(0).val.dateVal);
        } else if (i == 5) {
            auto d = Date::FromDate(1829, 10, 28);
            auto t = Time::FromTime( 17, 13, 37);
            EXPECT_EQ(Timestamp::FromDatetime(d, t), col->readValue(0).val.timestampVal);
        } else if (i == 6) {
            EXPECT_EQ("anDFrPZkcH", col->readValue(0).strVal);
        } else if (i == 7) {
            string interval = "3 years 1 day";
            EXPECT_EQ(Interval::FromCString(interval.c_str(), interval.length()), col->readValue(1).val.intervalVal);
        } else if (i == 8) {
            cout << TypeUtils::toString(col->readValue(0)) << endl;
        }
    }

}