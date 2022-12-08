#include "test_helper/test_helper.h"
#include <iostream>
#include <fstream>

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

    void checkElement(DataType type, string& element, Literal cell) {
        switch (type.typeID) {
            case INT64: {
                EXPECT_EQ(stoll(element), cell.val.int64Val);
            }
                break;
            case DOUBLE: {
                EXPECT_EQ(stod(element), cell.val.doubleVal);
            }
                break;
            case BOOL: {
                element[0] = toupper(element[0]);
                EXPECT_EQ(element, TypeUtils::toString(cell));
            }
                break;
            case STRING: {
                EXPECT_EQ(element, cell.strVal);
            }
                break;
            case DATE: {
                vector<string> splitDate = CopyCSVArrowNodeTest::splitStr(element, "-");
                auto d = Date::FromDate(stoi(splitDate[0]), stoi(splitDate[1]), stoi(splitDate[2]));
                EXPECT_EQ(d, cell.val.dateVal);
            }
                break;
            case TIMESTAMP: {
                int pos = element.find(' ');
                string  date = element.substr(0, pos);
                string time = element.substr(pos + 1);
                vector<string> splitDate = CopyCSVArrowNodeTest::splitStr(date, "-");
                auto d = Date::FromDate(stoi(splitDate[0]), stoi(splitDate[1]), stoi(splitDate[2]));

                vector<string> splitTime = CopyCSVArrowNodeTest::splitStr(time, ":");
                auto t = Time::FromTime( stoi(splitTime[0]), stoi(splitTime[1]), stoi(splitTime[2]));
                EXPECT_EQ(Timestamp::FromDatetime(d, t), cell.val.timestampVal);
            }
                break;
            case INTERVAL: {
                EXPECT_EQ(element, Interval::toString(cell.val.intervalVal));
            }
                break;
            case LIST: {

            }
                break;
            default:
                break;
        }
    }


    static vector<string> splitStr(string& s, string delimiter) {
        size_t pos = 0;
        std::string token;
        vector<string> split;
        while ((pos = s.find(delimiter)) != std::string::npos) {
            token = s.substr(0, pos);
            split.emplace_back(token);
            s.erase(0, pos + delimiter.length());
        }
        split.emplace_back(s);

        return split;
    }

    static vector<string> splitStrList(string& l, const char delimiter) {
        vector<string> split;
        int bracket = 0, last = 0;
        for (int i = 0; i < l.length(); i++) {
            if (l[i] == '"') {
                bracket += 1;
            } else if (bracket % 2 == 0 && l[i] == delimiter) {
                split.emplace_back(l.substr(last, i - last));
                last = i + 1;
            }
        }
        split.emplace_back(l.substr(last, l.length() - last));

//        cout << "size: " << split.size() << " " << "elements: " ;
//        for (auto& p : split) {
//            cout << p << ", ";
//        }
//        cout << endl;

        for (int i = 1; i < split.size(); ++i) {
            split[i] = split[i].substr(1, split[i].length() - 2);
        }

        return split;
    }
};


TEST_F(CopyCSVArrowNodeTest, ArrowCSVTest) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("arrow");
    bool header = true;

    ifstream MyReadFile("dataset/copy-csv-arrow-node-test/types_50k.csv");
    string  line;
    vector<string> lines;
    //auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("arrow_csv")

    if (header) {
        getline (MyReadFile, line);
    }
    while (getline (MyReadFile, line)) {
        lines.emplace_back(line);
    }

    for (int j = 0; j < lines.size(); j++) {
        vector<string> split = CopyCSVArrowNodeTest::splitStr(lines[j], ",");

        for (int i = 0; i <= 6; i++) {
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
            auto type = col->readValue(j).dataType;

            checkElement(type, split[i], col->readValue(j));
        }
    }
}

//TEST_F(CopyCSVArrowNodeTest, ListTest) {
//    auto storageManager = getStorageManager(*database);
//    auto catalog = getCatalog(*database);
//    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("arrow_list");
//
//    ifstream MyReadFile("dataset/copy-csv-arrow-node-test/list.csv");
//    string line;
//    vector<string> lines;
//    bool header = true;
//
//    if (header) {
//        getline (MyReadFile, line);
//    }
//    while (getline(MyReadFile, line)) {
//        lines.emplace_back(line);
//    }
//
//    for (int j = 0; j < lines.size(); j++) {
//        vector<string> split = CopyCSVArrowNodeTest::splitStrList(lines[j], ',');
//
//        for (int i = 0; i <= 6; i++) {
//            string name;
//            if (i == 0) {
//                name = "id";
//            } else {
//                name = "feature" + to_string(i);
//            }
//            auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(
//                    tableID, name);
//            auto col =
//                    storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
//            if (i == 0) {
//                EXPECT_EQ(stoll(split[i]), col->readValue(j).val.int64Val);
//            } else {
//                EXPECT_EQ(split[i], TypeUtils::toString(col->readValue(j)));
//            }
//        }
//    }
//}


TEST_F(CopyCSVArrowNodeTest, ArrowArrowTest) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("arrow_arrow");
    bool header = true;

    ifstream MyReadFile("dataset/copy-csv-arrow-node-test/types_50k.csv");
    string  line;
    vector<string> lines;
    //auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("arrow_csv")

    if (header) {
        getline (MyReadFile, line);
    }
    while (getline (MyReadFile, line)) {
        lines.emplace_back(line);
    }

    for (int j = 0; j < lines.size(); j++) {
        vector<string> split = CopyCSVArrowNodeTest::splitStr(lines[j], ",");

        for (int i = 0; i <= 6; i++) {
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
            auto type = col->readValue(j).dataType;

            checkElement(type, split[i], col->readValue(j));
        }
    }
}

TEST_F(CopyCSVArrowNodeTest, ArrowParquetTest) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("arrow_parquet");
    bool header = true;

    ifstream MyReadFile("dataset/copy-csv-arrow-node-test/types_50k.csv");
    string  line;
    vector<string> lines;
    //auto tableID = catalog->getReadOnlyVersion()->getNodeTableIDFromName("arrow_csv")

    if (header) {
        getline (MyReadFile, line);
    }
    while (getline (MyReadFile, line)) {
        lines.emplace_back(line);
    }

    for (int j = 0; j < lines.size(); j++) {
        vector<string> split = CopyCSVArrowNodeTest::splitStr(lines[j], ",");

        for (int i = 0; i <= 6; i++) {
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
            auto type = col->readValue(j).dataType;

            checkElement(type, split[i], col->readValue(j));
        }
    }
}
