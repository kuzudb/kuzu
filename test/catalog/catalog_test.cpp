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
        auto tableID = catalog->getReadOnlyVersion()->addNodeTableSchema(
            "person", 0 /* primaryKeyIdx */, move(personProperties));
        auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(tableID);
        nodeTableSchema->addUnstructuredProperties(unstrPropertyNames);

        vector<PropertyNameDataType> knowsProperties;
        knowsProperties.emplace_back("START_ID_TABLE", STRING);
        knowsProperties.emplace_back("START_ID", INT64);
        knowsProperties.emplace_back("END_ID_TABLE", STRING);
        knowsProperties.emplace_back("END_ID", INT64);
        knowsProperties.emplace_back("date", DATE);
        knowsProperties.emplace_back("meetTime", TIMESTAMP);
        knowsProperties.emplace_back("validInterval", INTERVAL);
        catalog->getReadOnlyVersion()->addRelTableSchema("knows", MANY_MANY, knowsProperties,
            SrcDstTableIDs{unordered_set<table_id_t>{0}, unordered_set<table_id_t>{0}});
    }

public:
    unique_ptr<Catalog> catalog;
};

TEST_F(CatalogTest, AddTablesTest) {
    // Test getting table id from string
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("person"));
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("organisation"));
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("knows"));
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getNodeTableIDFromName("person"), 0 /* node table ID */);
    ASSERT_EQ(catalog->getReadOnlyVersion()->getRelTableIDFromName("knows"), 0 /* rel table ID */);
    // Test rel single relMultiplicity
    ASSERT_FALSE(catalog->getReadOnlyVersion()->isSingleMultiplicityInDirection(0, FWD));
    // Test property definition
    // primary key of person table is a column name ID, which is at idx 0 in the predefined
    // properties
    ASSERT_EQ(0 /* pkPropertyIdx */,
        catalog->getReadOnlyVersion()->getNodeTableSchema(0 /* table ID */)->primaryKeyPropertyIdx);

    ASSERT_EQ(catalog->getReadOnlyVersion()->getNodeProperty(0, "age").propertyID, 5);
    ASSERT_EQ(catalog->getReadOnlyVersion()->getNodeProperty(0, "age").dataType.typeID, INT64);
    ASSERT_EQ(catalog->getReadOnlyVersion()->getNodeProperty(0, "birthdate").dataType.typeID, DATE);
    ASSERT_EQ(catalog->getReadOnlyVersion()->getNodeProperty(0, "registerTime").dataType.typeID,
        TIMESTAMP);
    ASSERT_EQ(catalog->getReadOnlyVersion()->getNodeProperty(0, "lastJobDuration").dataType.typeID,
        INTERVAL);

    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getNodeProperty(0, "workedHours").dataType.typeID, LIST);
    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getNodeProperty(0, "workedHours").dataType.childType->typeID,
        INT64);
    ASSERT_EQ(catalog->getReadOnlyVersion()->getNodeProperty(0, "usedNames").dataType.typeID, LIST);
    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getNodeProperty(0, "usedNames").dataType.childType->typeID,
        STRING);
    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getNodeProperty(0, "courseScoresPerTerm").dataType.typeID,
        LIST);

    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(0, "courseScoresPerTerm")
                  .dataType.childType->typeID,
        LIST);
    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(0, "courseScoresPerTerm")
                  .dataType.childType->childType->typeID,
        INT64);
    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getUnstrPropertiesNameToIdMap(0).at("unstrIntProp"), 0);
    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getAllNodeProperties(0)[13].dataType.typeID, UNSTRUCTURED);
    ASSERT_EQ(catalog->getReadOnlyVersion()->getRelProperty(0, "date").dataType.typeID, DATE);
    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getRelProperty(0, "meetTime").dataType.typeID, TIMESTAMP);
    ASSERT_EQ(catalog->getReadOnlyVersion()->getRelProperty(0, "validInterval").dataType.typeID,
        INTERVAL);
}

TEST_F(CatalogTest, SaveAndReadTest) {
    catalog->getReadOnlyVersion()->saveToFile(CATALOG_TEMP_DIRECTORY, DBFileType::ORIGINAL);
    auto newCatalog = make_unique<Catalog>();
    newCatalog->getReadOnlyVersion()->readFromFile(CATALOG_TEMP_DIRECTORY, DBFileType::ORIGINAL);
    /* primary key of person table is a column name ID, which is at idx 0 in the predefined
     * properties */
    ASSERT_EQ(0 /* pkPropertyIdx */, newCatalog->getReadOnlyVersion()
                                         ->getNodeTableSchema(0 /* table ID */)
                                         ->primaryKeyPropertyIdx);
    // Test getting table id from string
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("person"));
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("organisation"));
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("knows"));
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getUnstrPropertiesNameToIdMap(0).at("unstrIntProp"), 0);
}
