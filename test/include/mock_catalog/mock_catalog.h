#include "catalog/catalog.h"
#include "gmock/gmock.h"

static const char* PERSON_TABLE_STR = "person";
static const table_id_t PERSON_TABLE_ID = 0;
static const char* ORGANISATION_TABLE_STR = "organisation";
static const table_id_t ORGANISATION_TABLE_ID = 1;

static const char* KNOWS_TABLE_STR = "knows";
static const table_id_t KNOWS_TABLE_ID = 0;
static const char* WORKAT_TABLE_STR = "workAt";
static const table_id_t WORKAT_TABLE_ID = 1;

static const char* AGE_PROPERTY_KEY_STR = "age";
static const uint32_t AGE_PROPERTY_KEY_ID = 0;
static const char* NAME_PROPERTY_KEY_STR = "name";
static const uint32_t NAME_PROPERTY_KEY_ID = 1;
static const char* BIRTHDATE_PROPERTY_KEY_STR = "birthdate";
static const uint32_t BIRTHDATE_PROPERTY_KEY_ID = 2;
static const char* REGISTERTIME_PROPERTY_KEY_STR = "registerTime";
static const uint32_t REGISTERTIME_PROPERTY_KEY_ID = 3;
static const char* DESCRIPTION_PROPERTY_KEY_STR = "description";
static const uint32_t DESCRIPTION_PROPERTY_KEY_ID = 0;
static const char* KNOWSDATE_PROPERTY_KEY_STR = "knowsdate";
static const uint32_t KNOWSDATE_PROPERTY_KEY_ID = 1;

static const uint64_t NUM_PERSON_NODES = 10000;
static const uint64_t NUM_ORGANISATION_NODES = 100;

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::StrEq;
using ::testing::Throw;

using namespace kuzu::catalog;

class MockCatalogContent : public CatalogContent {

public:
    MOCK_METHOD(bool, containNodeProperty, (table_id_t tableID, const string& propertyName),
        (const, override));
    MOCK_METHOD(bool, containRelProperty, (table_id_t tableID, const string& propertyName),
        (const, override));
    MOCK_METHOD(const Property&, getNodeProperty, (table_id_t tableID, const string& propertyName),
        (const, override));
    MOCK_METHOD(const Property&, getRelProperty, (table_id_t tableID, const string& propertyName),
        (const, override));
    MOCK_METHOD(bool, isSingleMultiplicityInDirection,
        (table_id_t relTableID, RelDirection direction), (const, override));
    MOCK_METHOD(bool, containNodeTable, (const string& tableName), (const, override));
    MOCK_METHOD(bool, containRelTable, (const string& tableName), (const, override));
    MOCK_METHOD(table_id_t, getRelTableIDFromName, (const string& tableName), (const, override));
    MOCK_METHOD(table_id_t, getNodeTableIDFromName, (const string& tableName), (const, override));
    MOCK_METHOD(string, getNodeTableName, (table_id_t tableID), (const, override));
    MOCK_METHOD(string, getRelTableName, (table_id_t tableID), (const, override));
    MOCK_METHOD(uint64_t, getNumNodes, (table_id_t tableID), (const override));
    MOCK_METHOD(RelTableSchema*, getRelTableSchema, (table_id_t tableID), (const override));
};

/**
 * Mock tiny snb catalogContent with 2 node tables person and organisation
 * and 2 rel table knows and workAt.
 * Person has property age with type INT64.
 * Knows has property description with type STRING.
 */
class TinySnbCatalogContent : public MockCatalogContent {

public:
    void setUp() {
        setSrcNodeTableToRelTables();
        setDstNodeTableToRelTables();
        setRelTableSchemas();
        setProperties();
        setActionForContainNodeProperty();
        setActionForContainRelProperty();
        secActionForGetNodeProperty();
        secActionForGetRelProperty();
        setActionForIsSingleMultiplicityInDirection();
        setActionForContainNodeTable();
        setActionForContainRelTable();
        setActionForGetNodeTableFromString();
        setActionForGetRelTableFromString();
        setActionForGetNodeTableName();
        setActionForGetRelTableName();
        setActionForGetRelTableSchema();
    }

private:
    void setActionForContainNodeProperty() {
        ON_CALL(*this, containNodeProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containNodeProperty(PERSON_TABLE_ID, _)).WillByDefault(Return(false));
        ON_CALL(*this, containNodeProperty(PERSON_TABLE_ID, AGE_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containNodeProperty(PERSON_TABLE_ID, NAME_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containNodeProperty(PERSON_TABLE_ID, BIRTHDATE_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containNodeProperty(PERSON_TABLE_ID, REGISTERTIME_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containNodeProperty(ORGANISATION_TABLE_ID, _)).WillByDefault(Return(false));
    }

    void setActionForContainRelProperty() {
        ON_CALL(*this, containRelProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containRelProperty(KNOWS_TABLE_ID, _)).WillByDefault(Return(false));
        ON_CALL(*this, containRelProperty(KNOWS_TABLE_ID, DESCRIPTION_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containRelProperty(KNOWS_TABLE_ID, KNOWSDATE_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containRelProperty(KNOWS_TABLE_ID, KNOWSDATE_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containRelProperty(1, _)).WillByDefault(Return(false));
    }

    void secActionForGetNodeProperty() {
        ON_CALL(*this, getNodeProperty(PERSON_TABLE_ID, AGE_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(ageProperty));
        ON_CALL(*this, getNodeProperty(PERSON_TABLE_ID, NAME_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(nameProperty));
        ON_CALL(*this, getNodeProperty(PERSON_TABLE_ID, BIRTHDATE_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(birthDateProperty));
        ON_CALL(*this, getNodeProperty(PERSON_TABLE_ID, REGISTERTIME_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(registerTimeProperty));
    }

    void secActionForGetRelProperty() {
        ON_CALL(*this, getRelProperty(KNOWS_TABLE_ID, DESCRIPTION_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(descriptionProperty));
        ON_CALL(*this, getRelProperty(KNOWS_TABLE_ID, KNOWSDATE_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(knowsDateProperty));
    }

    void setActionForIsSingleMultiplicityInDirection() {
        ON_CALL(*this, isSingleMultiplicityInDirection(_, _)).WillByDefault(Return(false));
        ON_CALL(*this, isSingleMultiplicityInDirection(WORKAT_TABLE_ID, FWD))
            .WillByDefault(Return(true));
    }

    void setActionForContainNodeTable() {
        ON_CALL(*this, containNodeTable(_)).WillByDefault(Return(false));
        ON_CALL(*this, containNodeTable(StrEq(PERSON_TABLE_STR))).WillByDefault(Return(true));
        ON_CALL(*this, containNodeTable(StrEq(ORGANISATION_TABLE_STR))).WillByDefault(Return(true));
    }

    void setActionForContainRelTable() {
        ON_CALL(*this, containRelTable(_)).WillByDefault(Return(false));
        ON_CALL(*this, containRelTable(StrEq(KNOWS_TABLE_STR))).WillByDefault(Return(true));
        ON_CALL(*this, containRelTable(StrEq(WORKAT_TABLE_STR))).WillByDefault(Return(true));
    }

    void setActionForGetNodeTableFromString() {
        ON_CALL(*this, getNodeTableIDFromName(StrEq(PERSON_TABLE_STR)))
            .WillByDefault(Return(PERSON_TABLE_ID));
        ON_CALL(*this, getNodeTableIDFromName(StrEq(ORGANISATION_TABLE_STR)))
            .WillByDefault(Return(ORGANISATION_TABLE_ID));
    }

    void setActionForGetRelTableFromString() {
        ON_CALL(*this, getRelTableIDFromName(StrEq(KNOWS_TABLE_STR)))
            .WillByDefault(Return(KNOWS_TABLE_ID));
        ON_CALL(*this, getRelTableIDFromName(StrEq(WORKAT_TABLE_STR)))
            .WillByDefault(Return(WORKAT_TABLE_ID));
    }

    void setActionForGetNodeTableName() {
        ON_CALL(*this, getNodeTableName(PERSON_TABLE_ID)).WillByDefault(Return(PERSON_TABLE_STR));
        ON_CALL(*this, getNodeTableName(ORGANISATION_TABLE_ID))
            .WillByDefault(Return(ORGANISATION_TABLE_STR));
    }

    void setActionForGetRelTableName() {
        ON_CALL(*this, getRelTableName(KNOWS_TABLE_ID)).WillByDefault(Return(KNOWS_TABLE_STR));
        ON_CALL(*this, getRelTableName(WORKAT_TABLE_ID)).WillByDefault(Return(WORKAT_TABLE_STR));
    }

    void setActionForGetRelTableSchema() {
        ON_CALL(*this, getRelTableSchema(KNOWS_TABLE_ID))
            .WillByDefault(Return(knowsTableSchema.get()));
        ON_CALL(*this, getRelTableSchema(WORKAT_TABLE_ID))
            .WillByDefault(Return(workAtTableSchema.get()));
    }

    void setSrcNodeTableToRelTables() {
        unordered_set<table_id_t> personToRelTableIDs = {KNOWS_TABLE_ID, WORKAT_TABLE_ID};
        unordered_set<table_id_t> organisationToRelTableIDs = {};
        srcNodeIDToRelIDs.push_back(move(personToRelTableIDs));
        srcNodeIDToRelIDs.push_back(move(organisationToRelTableIDs));
    }

    void setDstNodeTableToRelTables() {
        unordered_set<table_id_t> personToRelTableIDs = {KNOWS_TABLE_ID};
        unordered_set<table_id_t> organisationToRelTableIDs = {WORKAT_TABLE_ID};
        dstNodeIDToRelIDs.push_back(move(personToRelTableIDs));
        dstNodeIDToRelIDs.push_back(move(organisationToRelTableIDs));
    }

    void setProperties() {
        PropertyNameDataType agePropertyDefinition(AGE_PROPERTY_KEY_STR, INT64);
        ageProperty = Property::constructStructuredNodeProperty(
            agePropertyDefinition, AGE_PROPERTY_KEY_ID, PERSON_TABLE_ID);
        PropertyNameDataType namePropertyDefinition(NAME_PROPERTY_KEY_STR, STRING);
        nameProperty = Property::constructStructuredNodeProperty(
            namePropertyDefinition, NAME_PROPERTY_KEY_ID, PERSON_TABLE_ID);
        PropertyNameDataType birthDatePropertyDefinition(BIRTHDATE_PROPERTY_KEY_STR, DATE);
        birthDateProperty = Property::constructStructuredNodeProperty(
            birthDatePropertyDefinition, BIRTHDATE_PROPERTY_KEY_ID, PERSON_TABLE_ID);
        PropertyNameDataType registerTimePropertyDefinition(
            REGISTERTIME_PROPERTY_KEY_STR, TIMESTAMP);
        registerTimeProperty = Property::constructStructuredNodeProperty(
            registerTimePropertyDefinition, REGISTERTIME_PROPERTY_KEY_ID, PERSON_TABLE_ID);
        PropertyNameDataType descriptionPropertyDefinition(DESCRIPTION_PROPERTY_KEY_STR, STRING);
        descriptionProperty = Property::constructRelProperty(
            descriptionPropertyDefinition, DESCRIPTION_PROPERTY_KEY_ID, KNOWS_TABLE_ID);
        PropertyNameDataType knowsDatePropertyDefinition(KNOWSDATE_PROPERTY_KEY_STR, DATE);
        knowsDateProperty = Property::constructRelProperty(
            knowsDatePropertyDefinition, KNOWSDATE_PROPERTY_KEY_ID, KNOWS_TABLE_ID);
    }

    void setRelTableSchemas() {
        auto knowsID = Property::constructRelProperty(
            PropertyNameDataType(INTERNAL_ID_SUFFIX, INT64), 0, KNOWS_TABLE_ID);
        knowsTableSchema = make_unique<RelTableSchema>("knows", KNOWS_TABLE_ID, MANY_MANY,
            vector<Property>{knowsID},
            vector<pair<table_id_t, table_id_t>>{{PERSON_TABLE_ID, PERSON_TABLE_ID}});
        auto workAtID = Property::constructRelProperty(
            PropertyNameDataType(INTERNAL_ID_SUFFIX, INT64), 0, WORKAT_TABLE_ID);
        workAtTableSchema = make_unique<RelTableSchema>("workAt", WORKAT_TABLE_ID, MANY_ONE,
            vector<Property>{workAtID},
            vector<pair<table_id_t, table_id_t>>{{PERSON_TABLE_ID, ORGANISATION_TABLE_ID}});
    }

    vector<unordered_set<table_id_t>> srcNodeIDToRelIDs, dstNodeIDToRelIDs;
    Property ageProperty, nameProperty, descriptionProperty, birthDateProperty,
        registerTimeProperty, knowsDateProperty;
    unique_ptr<RelTableSchema> knowsTableSchema;
    unique_ptr<RelTableSchema> workAtTableSchema;
};

class TinySnbCatalog : public Catalog {
public:
    void setUp() {
        auto catalogContent = make_unique<NiceMock<TinySnbCatalogContent>>();
        catalogContent->setUp();
        catalogContentForReadOnlyTrx = move(catalogContent);
    }
};
