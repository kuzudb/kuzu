#include "catalog/catalog.h"
#include "gtest/gtest.h"
#include "test_helper/test_helper.h"

using namespace std;
using namespace kuzu::catalog;

class CatalogTest : public testing::Test {
public:
    const string CATALOG_TEMP_DIRECTORY =
        kuzu::testing::TestHelper::appendKuzuRootPath("test/catalog_temp");

    void SetUp() override {
        FileUtils::createDir(CATALOG_TEMP_DIRECTORY);
        catalog = make_unique<Catalog>();
        setupCatalog();
    }

    void TearDown() override { FileUtils::removeDir(CATALOG_TEMP_DIRECTORY); }

    void setupCatalog() {
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
        PERSON_TABLE_ID = catalog->getReadOnlyVersion()->addNodeTableSchema(
            "person", 0 /* primaryKeyIdx */, move(personProperties));
        auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(PERSON_TABLE_ID);

        vector<PropertyNameDataType> knowsProperties;
        knowsProperties.emplace_back("START_ID_TABLE", STRING);
        knowsProperties.emplace_back("START_ID", INT64);
        knowsProperties.emplace_back("END_ID_TABLE", STRING);
        knowsProperties.emplace_back("END_ID", INT64);
        knowsProperties.emplace_back("date", DATE);
        knowsProperties.emplace_back("meetTime", TIMESTAMP);
        knowsProperties.emplace_back("validInterval", INTERVAL);
        KNOWS_TABLE_ID =
            catalog->getReadOnlyVersion()->addRelTableSchema("knows", MANY_MANY, knowsProperties,
                vector<pair<table_id_t, table_id_t>>{{0 /* srcTableID */, 0 /* dstTableID */}});
    }

public:
    unique_ptr<Catalog> catalog;
    table_id_t PERSON_TABLE_ID;
    table_id_t KNOWS_TABLE_ID;
};

TEST_F(CatalogTest, AddTablesTest) {
    // Test getting table id from string
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("person"));
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("organisation"));
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("knows"));
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
    ASSERT_EQ(catalog->getReadOnlyVersion()->getNodeTableIDFromName("person"), PERSON_TABLE_ID);
    ASSERT_EQ(catalog->getReadOnlyVersion()->getRelTableIDFromName("knows"), KNOWS_TABLE_ID);
    ASSERT_NE(PERSON_TABLE_ID, KNOWS_TABLE_ID);
    // Test rel single relMultiplicity
    ASSERT_FALSE(
        catalog->getReadOnlyVersion()->isSingleMultiplicityInDirection(KNOWS_TABLE_ID, FWD));
    // Test property definition
    // primary key of person table is a column name ID, which is at idx 0 in the predefined
    // properties
    ASSERT_EQ(0 /* pkPropertyIdx */,
        catalog->getReadOnlyVersion()->getNodeTableSchema(PERSON_TABLE_ID)->primaryKeyPropertyIdx);

    ASSERT_EQ(catalog->getReadOnlyVersion()->getNodeProperty(PERSON_TABLE_ID, "age").propertyID, 5);
    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getNodeProperty(PERSON_TABLE_ID, "age").dataType.typeID,
        INT64);
    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(PERSON_TABLE_ID, "birthdate")
                  .dataType.typeID,
        DATE);
    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(PERSON_TABLE_ID, "registerTime")
                  .dataType.typeID,
        TIMESTAMP);
    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(PERSON_TABLE_ID, "lastJobDuration")
                  .dataType.typeID,
        INTERVAL);

    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(PERSON_TABLE_ID, "workedHours")
                  .dataType.typeID,
        LIST);
    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(PERSON_TABLE_ID, "workedHours")
                  .dataType.childType->typeID,
        INT64);
    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(PERSON_TABLE_ID, "usedNames")
                  .dataType.typeID,
        LIST);
    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(PERSON_TABLE_ID, "usedNames")
                  .dataType.childType->typeID,
        STRING);
    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(PERSON_TABLE_ID, "courseScoresPerTerm")
                  .dataType.typeID,
        LIST);

    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(PERSON_TABLE_ID, "courseScoresPerTerm")
                  .dataType.childType->typeID,
        LIST);
    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getNodeProperty(PERSON_TABLE_ID, "courseScoresPerTerm")
                  .dataType.childType->childType->typeID,
        INT64);
    ASSERT_EQ(catalog->getReadOnlyVersion()->getRelProperty(KNOWS_TABLE_ID, "date").dataType.typeID,
        DATE);
    ASSERT_EQ(
        catalog->getReadOnlyVersion()->getRelProperty(KNOWS_TABLE_ID, "meetTime").dataType.typeID,
        TIMESTAMP);
    ASSERT_EQ(catalog->getReadOnlyVersion()
                  ->getRelProperty(KNOWS_TABLE_ID, "validInterval")
                  .dataType.typeID,
        INTERVAL);
}

TEST_F(CatalogTest, SaveAndReadTest) {
    catalog->getReadOnlyVersion()->saveToFile(CATALOG_TEMP_DIRECTORY, DBFileType::ORIGINAL);
    auto newCatalog = make_unique<Catalog>();
    newCatalog->getReadOnlyVersion()->readFromFile(CATALOG_TEMP_DIRECTORY, DBFileType::ORIGINAL);
    /* primary key of person table is a column name ID, which is at idx 0 in the predefined
     * properties */
    ASSERT_EQ(0 /* pkPropertyIdx */, newCatalog->getReadOnlyVersion()
                                         ->getNodeTableSchema(PERSON_TABLE_ID)
                                         ->primaryKeyPropertyIdx);
    // Test getting table id from string
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containNodeTable("person"));
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containNodeTable("organisation"));
    ASSERT_TRUE(catalog->getReadOnlyVersion()->containRelTable("knows"));
    ASSERT_FALSE(catalog->getReadOnlyVersion()->containRelTable("likes"));
}
