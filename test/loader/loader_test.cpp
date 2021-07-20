#include "test/test_utility/include/test_helper.h"

#include "src/common/include/csv_reader/csv_reader.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class LoaderTest : public InMemoryDBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/loader_test/"; }
};

TEST_F(LoaderTest, NodeStructuredStringPropertyTest) {
    auto& catalog = defaultSystem->graph->getCatalog();
    auto label = catalog.getNodeLabelFromString("person");
    auto propertyIdx = catalog.getNodeProperty(label, "randomString");
    auto column = reinterpret_cast<PropertyColumnString*>(
        defaultSystem->graph->getNodesStore().getNodePropertyColumn(label, propertyIdx.id));
    string fName = getInputCSVDir() + "vPerson.csv";
    CSVReader csvReader(fName, ',');
    int lineIdx = 0;
    csvReader.hasNextLine();
    csvReader.skipLine();
    NumericMetric numBufferHits(true), numBufferMisses(true), numIO(true);
    BufferManagerMetrics metrics(numBufferHits, numBufferMisses, numIO);
    while (csvReader.hasNextLine()) {
        csvReader.hasNextToken();
        csvReader.skipToken();
        csvReader.hasNextToken();
        EXPECT_EQ(string(csvReader.getString()), column->readValue(lineIdx, metrics).strVal);
        lineIdx++;
        csvReader.skipLine();
    }
}

TEST_F(LoaderTest, NodeUnstructuredPropertyTest) {
    auto& catalog = defaultSystem->graph->getCatalog();
    auto label = catalog.getNodeLabelFromString("person");
    auto lists = reinterpret_cast<UnstructuredPropertyLists*>(
        defaultSystem->graph->getNodesStore().getNodeUnstrPropertyLists(label));
    // BufferManagerMetrics and Page Handle is required only if the database is not loaded in mem.
    NumericMetric numBufferHits(true), numBufferMisses(true), numIO(true);
    BufferManagerMetrics metrics(numBufferHits, numBufferMisses, numIO);
    auto pageHandle = make_unique<PageHandle>();
    auto& propertyNameToIdMap = catalog.getUnstrPropertiesNameToIdMap(label);
    for (int i = 0; i < 1000; ++i) {
        auto propertiesMap = lists->readList(i, pageHandle, metrics);
        if (i == 300 || i == 400 || i == 500) {
            EXPECT_EQ(i * 4, propertiesMap->size());
            for (int j = 0; j < i; ++j) {
                EXPECT_EQ("strPropVal" + to_string(j),
                    propertiesMap->at(propertyNameToIdMap.at("strPropKey" + to_string(j))).strVal);
                EXPECT_EQ(
                    j, propertiesMap->at(propertyNameToIdMap.at("int64PropKey" + to_string(j)))
                           .val.int64Val);
                EXPECT_EQ(j * 1.0,
                    propertiesMap->at(propertyNameToIdMap.at("doublePropKey" + to_string(j)))
                        .val.doubleVal);
                EXPECT_FALSE(propertiesMap->at(propertyNameToIdMap.at("boolPropKey" + to_string(j)))
                                 .val.booleanVal);
            }
        } else {
            EXPECT_EQ(4, propertiesMap->size());
            EXPECT_EQ(
                "strPropVal1", propertiesMap->at(propertyNameToIdMap.at("strPropKey1")).strVal);
            EXPECT_EQ(1, propertiesMap->at(propertyNameToIdMap.at("int64PropKey1")).val.int64Val);
            EXPECT_EQ(
                1.0, propertiesMap->at(propertyNameToIdMap.at("doublePropKey1")).val.doubleVal);
            EXPECT_TRUE(propertiesMap->at(propertyNameToIdMap.at("boolPropKey1")).val.booleanVal);
        }
    }
    lists->reclaim(*pageHandle);
}
