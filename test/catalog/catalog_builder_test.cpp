#include "include/gtest/gtest.h"

#include "src/catalog/include/catalog.h"

using namespace std;
using namespace graphflow::catalog;

class CatalogBuilderTest : public testing::Test {
public:
    const string CATALOG_TEMP_DIRECTORY = "test/catalog_temp";

    void SetUp() override {
        FileUtils::createDir(CATALOG_TEMP_DIRECTORY);
        catalogBuilder = make_unique<CatalogBuilder>();
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
        auto nodeLabel = catalogBuilder->createNodeLabel("person", "ID", move(personProperties));
        nodeLabel->addUnstructuredProperties(unstrPropertyNames);
        catalogBuilder->addNodeLabel(move(nodeLabel));

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
        auto relLabel = catalogBuilder->createRelLabel(
            "knows", MANY_MANY, knowsProperties, knowsSrcNodeLabelNames, knowsDstNodeLabelNames);
        catalogBuilder->addRelLabel(move(relLabel));
    }

public:
    unique_ptr<CatalogBuilder> catalogBuilder;
};

TEST_F(CatalogBuilderTest, AddLabelsTest) {
    // Test getting label id from string
    ASSERT_TRUE(catalogBuilder->getReadOnlyVersion()->containNodeLabel("person"));
    ASSERT_FALSE(catalogBuilder->getReadOnlyVersion()->containNodeLabel("organisation"));
    ASSERT_TRUE(catalogBuilder->getReadOnlyVersion()->containRelLabel("knows"));
    ASSERT_FALSE(catalogBuilder->getReadOnlyVersion()->containRelLabel("likes"));
    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()->getNodeLabelFromName("person"), 0);
    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()->getRelLabelFromName("knows"), 0);
    // Test rel single relMultiplicity
    ASSERT_FALSE(catalogBuilder->getReadOnlyVersion()->isSingleMultiplicityInDirection(0, FWD));
    // Test property definition
    ASSERT_TRUE(
        catalogBuilder->getReadOnlyVersion()->getStructuredNodeProperties(0)[0].isIDProperty());

    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()->getNodeProperty(0, "age").propertyID, 5);
    ASSERT_EQ(
        catalogBuilder->getReadOnlyVersion()->getNodeProperty(0, "age").dataType.typeID, INT64);
    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()->getNodeProperty(0, "birthdate").dataType.typeID,
        DATE);
    ASSERT_EQ(
        catalogBuilder->getReadOnlyVersion()->getNodeProperty(0, "registerTime").dataType.typeID,
        TIMESTAMP);
    ASSERT_EQ(
        catalogBuilder->getReadOnlyVersion()->getNodeProperty(0, "lastJobDuration").dataType.typeID,
        INTERVAL);

    ASSERT_EQ(
        catalogBuilder->getReadOnlyVersion()->getNodeProperty(0, "workedHours").dataType.typeID,
        LIST);
    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()
                  ->getNodeProperty(0, "workedHours")
                  .dataType.childType->typeID,
        INT64);
    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()->getNodeProperty(0, "usedNames").dataType.typeID,
        LIST);
    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()
                  ->getNodeProperty(0, "usedNames")
                  .dataType.childType->typeID,
        STRING);
    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()
                  ->getNodeProperty(0, "courseScoresPerTerm")
                  .dataType.typeID,
        LIST);

    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()
                  ->getNodeProperty(0, "courseScoresPerTerm")
                  .dataType.childType->typeID,
        LIST);
    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()
                  ->getNodeProperty(0, "courseScoresPerTerm")
                  .dataType.childType->childType->typeID,
        INT64);
    ASSERT_EQ(
        catalogBuilder->getReadOnlyVersion()->getUnstrPropertiesNameToIdMap(0).at("unstrIntProp"),
        0);
    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()->getAllNodeProperties(0)[13].dataType.typeID,
        UNSTRUCTURED);
    ASSERT_EQ(
        catalogBuilder->getReadOnlyVersion()->getRelProperty(0, "date").dataType.typeID, DATE);
    ASSERT_EQ(catalogBuilder->getReadOnlyVersion()->getRelProperty(0, "meetTime").dataType.typeID,
        TIMESTAMP);
    ASSERT_EQ(
        catalogBuilder->getReadOnlyVersion()->getRelProperty(0, "validInterval").dataType.typeID,
        INTERVAL);
}

TEST_F(CatalogBuilderTest, SaveAndReadTest) {
    catalogBuilder->getReadOnlyVersion()->saveToFile(
        CATALOG_TEMP_DIRECTORY, false /* isForWALRecord */);
    auto newCatalog = make_unique<CatalogBuilder>();
    newCatalog->getReadOnlyVersion()->readFromFile(CATALOG_TEMP_DIRECTORY);
    ASSERT_TRUE(newCatalog->getReadOnlyVersion()->getStructuredNodeProperties(0)[0].isIDProperty());
    // Test getting label id from string
    ASSERT_TRUE(catalogBuilder->getReadOnlyVersion()->containNodeLabel("person"));
    ASSERT_FALSE(catalogBuilder->getReadOnlyVersion()->containNodeLabel("organisation"));
    ASSERT_TRUE(catalogBuilder->getReadOnlyVersion()->containRelLabel("knows"));
    ASSERT_FALSE(catalogBuilder->getReadOnlyVersion()->containRelLabel("likes"));
    ASSERT_EQ(
        catalogBuilder->getReadOnlyVersion()->getUnstrPropertiesNameToIdMap(0).at("unstrIntProp"),
        0);
}
