#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"
#include "test/storage/include/node_property_file_scanner.h"
#include "test/test_utility/include/test_helper.h"

#include "src/common/include/csv_reader/csv_reader.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class LoadStringPropertyTest : public BaseGraphLoadingTest {

public:
    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        catalog = make_unique<Catalog>(TEMP_TEST_DIR);
    }

    string getInputCSVDir() override { return "dataset/string_column_test/"; }

public:
    unique_ptr<Catalog> catalog;
};

TEST_F(LoadStringPropertyTest, NodePropertyColumnWithDate) {
    StructuredStringNodePropertyFileScanner scanner(
        TEMP_TEST_DIR, catalog->getNodeLabelFromString("person"), "randomString");
    string fName = getInputCSVDir() + "vPerson.csv";
    CSVReader csvReader(fName, ',');
    int lineIdx = 0;
    csvReader.hasNextLine();
    csvReader.skipLine();
    while (csvReader.hasNextLine()) {
        csvReader.hasNextToken();
        csvReader.skipToken();
        csvReader.hasNextToken();
        EXPECT_EQ(string(csvReader.getString()), scanner.readString(lineIdx));
        lineIdx++;
        csvReader.skipLine();
    }
}
