#include "include/gtest/gtest.h"

#include "src/catalog/include/catalog.h"

using namespace std;
using namespace graphflow::catalog;

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
        vector<PropertyNameDataType> personProperties;
        personProperties.emplace_back("ID", INT64);
        personProperties.emplace_back("fName", STRING);
        personProperties.emplace_back("gender", INT64);
        personProperties.emplace_back("isStudent", BOOL);
        personProperties.emplace_back("isWorker", BOOL);
        personProperties.emplace_back("age", INT64);
        personProperties.emplace_back("eyeSight", DOUBLE);
        personProperties.emplace_back("birthdate", DATE);
        personProperties.emplace_back("registerTime", TIMESTAMP);
        personProperties.emplace_back("lastJobDuration", INTERVAL);
        personProperties.emplace_back("workedHours", DataType(LIST, make_unique<DataType>(INT64)));
        personProperties.emplace_back("usedNames", DataType(LIST, make_unique<DataType>(STRING)));
        personProperties.emplace_back("courseScoresPerTerm",
            DataType(LIST, make_unique<DataType>(LIST, make_unique<DataType>(INT64))));
        vector<string> unstrPropertyNames{"unstrIntProp"};
        auto nodeLabel = catalog->createNodeLabel("person", "ID", move(personProperties));
        nodeLabel->addUnstructuredProperties(unstrPropertyNames);
        catalog->addNodeLabel(move(nodeLabel));

        vector<PropertyNameDataType> knowsProperties;
        knowsProperties.emplace_back("START_ID_LABEL", STRING);
        knowsProperties.emplace_back("START_ID", INT64);
        knowsProperties.emplace_back("END_ID_LABEL", STRING);
        knowsProperties.emplace_back("END_ID", INT64);
        knowsProperties.emplace_back("date", DATE);
        knowsProperties.emplace_back("meetTime", TIMESTAMP);
        knowsProperties.emplace_back("validInterval", INTERVAL);
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
    ASSERT_EQ(catalog->getNodeLabelFromName("person"), 0);
    ASSERT_EQ(catalog->getRelLabelFromName("knows"), 0);
    // Test rel single relMultiplicity
    ASSERT_FALSE(catalog->isSingleMultiplicityInDirection(0, FWD));
    // Test property definition
    ASSERT_TRUE(catalog->getStructuredNodeProperties(0)[0].isIDProperty());

    ASSERT_EQ(catalog->getNodeProperty(0, "age").propertyID, 5);
    ASSERT_EQ(catalog->getNodeProperty(0, "age").dataType.typeID, INT64);
    ASSERT_EQ(catalog->getNodeProperty(0, "birthdate").dataType.typeID, DATE);
    ASSERT_EQ(catalog->getNodeProperty(0, "registerTime").dataType.typeID, TIMESTAMP);
    ASSERT_EQ(catalog->getNodeProperty(0, "lastJobDuration").dataType.typeID, INTERVAL);

    ASSERT_EQ(catalog->getNodeProperty(0, "workedHours").dataType.typeID, LIST);
    ASSERT_EQ(catalog->getNodeProperty(0, "workedHours").dataType.childType->typeID, INT64);
    ASSERT_EQ(catalog->getNodeProperty(0, "usedNames").dataType.typeID, LIST);
    ASSERT_EQ(catalog->getNodeProperty(0, "usedNames").dataType.childType->typeID, STRING);
    ASSERT_EQ(catalog->getNodeProperty(0, "courseScoresPerTerm").dataType.typeID, LIST);

    ASSERT_EQ(catalog->getNodeProperty(0, "courseScoresPerTerm").dataType.childType->typeID, LIST);
    ASSERT_EQ(
        catalog->getNodeProperty(0, "courseScoresPerTerm").dataType.childType->childType->typeID,
        INT64);
    ASSERT_EQ(catalog->getUnstrPropertiesNameToIdMap(0).at("unstrIntProp"), 0);
    ASSERT_EQ(catalog->getAllNodeProperties(0)[13].dataType.typeID, UNSTRUCTURED);
    ASSERT_EQ(catalog->getRelProperty(0, "date").dataType.typeID, DATE);
    ASSERT_EQ(catalog->getRelProperty(0, "meetTime").dataType.typeID, TIMESTAMP);
    ASSERT_EQ(catalog->getRelProperty(0, "validInterval").dataType.typeID, INTERVAL);
}

TEST_F(CatalogTest, SaveAndReadTest) {
    catalog->saveToFile(CATALOG_TEMP_DIRECTORY);
    auto newCatalog = make_unique<Catalog>();
    newCatalog->readFromFile(CATALOG_TEMP_DIRECTORY);
    ASSERT_TRUE(newCatalog->getStructuredNodeProperties(0)[0].isIDProperty());
    // Test getting label id from string
    ASSERT_TRUE(catalog->containNodeLabel("person"));
    ASSERT_FALSE(catalog->containNodeLabel("organisation"));
    ASSERT_TRUE(catalog->containRelLabel("knows"));
    ASSERT_FALSE(catalog->containRelLabel("likes"));
    ASSERT_EQ(catalog->getUnstrPropertiesNameToIdMap(0).at("unstrIntProp"), 0);
}
