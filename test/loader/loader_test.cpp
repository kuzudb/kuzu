#include "test/storage/include/file_scanners/unstructured_node_property_file_scanner.h"
#include "test/test_utility/include/test_helper.h"

#include "src/common/include/csv_reader/csv_reader.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class LoaderDatasetTest : public InMemoryDBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/loader_test/"; }
};

TEST_F(LoaderDatasetTest, NodeStructuredStringPropertyTest) {
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

// TEST_F(LoaderNodeStrStringAndUnstrPropertyTest, NodeUnstructuredPropertyTest) {
//    UnstructuredNodePropertyFileScanner scanner{
//        TEMP_TEST_DIR, catalog->getNodeLabelFromString("person")};
//    for (int i = 0; i < 1000; ++i) {
//        auto m = scanner.readUnstructuredProperties(i);
//        if (i == 300 || i == 400 || i == 500) {
//            EXPECT_EQ(i * 4, m.size());
//            for (int j = 0; j < i; ++j) {
//                EXPECT_EQ("strPropVal" + to_string(j), m["strPropKey" + to_string(j)].strVal);
//                EXPECT_EQ(j, m["int64PropKey" + to_string(j)].val.int64Val);
//                EXPECT_EQ(j * 1.0, m["doublePropKey" + to_string(j)].val.doubleVal);
//                EXPECT_TRUE(m.contains("boolPropKey" + to_string(j)));
//                EXPECT_FALSE(m["boolPropKey" + to_string(j)].val.booleanVal);
//            }
//        } else {
//            EXPECT_EQ(4, m.size());
//            EXPECT_EQ("strPropVal1", m["strPropKey1"].strVal);
//            EXPECT_EQ(1, m["int64PropKey1"].val.int64Val);
//            EXPECT_EQ(1.0, m["doublePropKey1"].val.doubleVal);
//            EXPECT_TRUE(m.contains("boolPropKey1"));
//            EXPECT_TRUE(m["boolPropKey1"].val.booleanVal);
//        }
//    }
//}
