#include "gtest/gtest.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"
#include "test/test_utility/include/test_helper.h"

#include "src/storage/include/catalog.h"

using namespace std;
using namespace graphflow::storage;
using namespace graphflow::testing;

class CatalogTest : public testing::Test {
public:
    const string CATALOG_TEMP_DIRECTORY = "test/catalog_temp";

    void SetUp() override {
        FileUtils::createDir(CATALOG_TEMP_DIRECTORY);
        catalog = make_unique<Catalog>();
        setupCatalog();
    }

    void TearDown() override { FileUtils::removeDir(CATALOG_TEMP_DIRECTORY); }

    void setupCatalog() const {
        vector<PropertyDefinition> personProperties;
        personProperties.emplace_back("id", 0, INT64);
        personProperties.emplace_back("fName", 1, STRING);
        personProperties.emplace_back("gender", 2, INT64);
        personProperties.emplace_back("isStudent", 3, BOOL);
        personProperties.emplace_back("isWorker", 4, BOOL);
        personProperties.emplace_back("age", 5, INT64);
        personProperties.emplace_back("eyeSight", 6, DOUBLE);
        catalog->addNodeLabel("person", move(personProperties), "id");
        catalog->addNodeUnstrProperty(0, "unstrIntProp");

        vector<PropertyDefinition> knowsProperties;
        knowsProperties.emplace_back("date", 0, INT64);
        vector<string> knowsSrcNodeLabelNames = {"person"};
        vector<string> knowsDstNodeLabelNames = {"person"};
        catalog->addRelLabel("knows", MANY_MANY, move(knowsProperties), knowsSrcNodeLabelNames,
            knowsDstNodeLabelNames);
    }

public:
    unique_ptr<Catalog> catalog;
};

TEST_F(CatalogTest, AddLabelsTest) {
    // Test getting label id from string
    ASSERT_TRUE(catalog->containNodeLabel("person"));
    ASSERT_FALSE(catalog->containNodeLabel("organisation"));
    ASSERT_TRUE(catalog->containRelLabel("knows"));
    ASSERT_FALSE(catalog->containRelLabel("likes"));
    ASSERT_EQ(catalog->getNodeLabelFromString("person"), 0);
    ASSERT_EQ(catalog->getRelLabelFromString("knows"), 0);
    // Test rel single relMultiplicity
    ASSERT_FALSE(catalog->isSingleMultiplicityInDirection(0, FWD));
    // Test property definition
    ASSERT_TRUE(catalog->getStructuredNodeProperties(0)[0].isPrimaryKey);
    ASSERT_EQ(catalog->getNodeProperty(0, "age").id, 5);
    ASSERT_EQ(catalog->getNodeProperty(0, "age").dataType, INT64);
    ASSERT_EQ(catalog->getUnstrPropertiesNameToIdMap(0).at("unstrIntProp"), 0);
    auto a = catalog->getAllNodeProperties(0);
    for (auto& property : a) {
        cout << property.name << " " << property.id << property.dataType << endl;
    }
    ASSERT_EQ(catalog->getAllNodeProperties(0)[7].dataType, UNSTRUCTURED);
    ASSERT_EQ(catalog->getRelProperty(0, "date").dataType, INT64);
}

TEST_F(CatalogTest, SaveAndReadTest) {
    catalog->saveToFile(CATALOG_TEMP_DIRECTORY);
    auto newCatalog = make_unique<Catalog>();
    newCatalog->readFromFile(CATALOG_TEMP_DIRECTORY);
    ASSERT_TRUE(newCatalog->getStructuredNodeProperties(0)[0].isPrimaryKey);
    // Test getting label id from string
    ASSERT_TRUE(catalog->containNodeLabel("person"));
    ASSERT_FALSE(catalog->containNodeLabel("organisation"));
    ASSERT_TRUE(catalog->containRelLabel("knows"));
    ASSERT_FALSE(catalog->containRelLabel("likes"));
    ASSERT_EQ(catalog->getUnstrPropertiesNameToIdMap(0).at("unstrIntProp"), 0);
}
