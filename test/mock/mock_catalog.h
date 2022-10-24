#include "gmock/gmock.h"

#include "src/catalog/include/catalog.h"

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

using namespace graphflow::catalog;

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
    MOCK_METHOD(const unordered_set<table_id_t>&, getRelTableIDsForNodeTableDirection,
        (table_id_t nodeTableID, RelDirection direction), (const, override));
    MOCK_METHOD(bool, isSingleMultiplicityInDirection,
        (table_id_t relTableID, RelDirection direction), (const, override));
    MOCK_METHOD(bool, containNodeTable, (const string& tableName), (const, override));
    MOCK_METHOD(bool, containRelTable, (const string& tableName), (const, override));
    MOCK_METHOD(table_id_t, getRelTableIDFromName, (const string& tableName), (const, override));
    MOCK_METHOD(table_id_t, getNodeTableIDFromName, (const string& tableName), (const, override));
    MOCK_METHOD(string, getNodeTableName, (table_id_t tableID), (const, override));
    MOCK_METHOD(string, getRelTableName, (table_id_t tableID), (const, override));
    MOCK_METHOD(uint64_t, getNumNodes, (table_id_t tableID), (const override));
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
        setProperties();

        setActionForContainNodeProperty();
        setActionForContainRelProperty();
        secActionForGetNodeProperty();
        secActionForGetRelProperty();
        setActionForGetRelTablesForNodeTableDirection();
        setActionForIsSingleMultiplicityInDirection();
        setActionForContainNodeTable();
        setActionForContainRelTable();
        setActionForGetNodeTableFromString();
        setActionForGetRelTableFromString();
        setActionForGetNodeTableName();
        setActionForGetRelTableName();
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

    void setActionForGetRelTablesForNodeTableDirection() {
        ON_CALL(*this, getRelTableIDsForNodeTableDirection(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getRelTableIDsForNodeTableDirection(PERSON_TABLE_ID, FWD))
            .WillByDefault(ReturnRef(srcNodeIDToRelIDs[PERSON_TABLE_ID]));
        ON_CALL(*this, getRelTableIDsForNodeTableDirection(PERSON_TABLE_ID, BWD))
            .WillByDefault(ReturnRef(dstNodeIDToRelIDs[PERSON_TABLE_ID]));
        ON_CALL(*this, getRelTableIDsForNodeTableDirection(ORGANISATION_TABLE_ID, FWD))
            .WillByDefault(ReturnRef(srcNodeIDToRelIDs[ORGANISATION_TABLE_ID]));
        ON_CALL(*this, getRelTableIDsForNodeTableDirection(ORGANISATION_TABLE_ID, BWD))
            .WillByDefault(ReturnRef(dstNodeIDToRelIDs[ORGANISATION_TABLE_ID]));
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
        ageProperty =
            Property(AGE_PROPERTY_KEY_STR, DataType(INT64), AGE_PROPERTY_KEY_ID, PERSON_TABLE_ID);
        nameProperty = Property(
            NAME_PROPERTY_KEY_STR, DataType(STRING), NAME_PROPERTY_KEY_ID, PERSON_TABLE_ID);
        birthDateProperty = Property(
            BIRTHDATE_PROPERTY_KEY_STR, DataType(DATE), BIRTHDATE_PROPERTY_KEY_ID, PERSON_TABLE_ID);
        registerTimeProperty = Property(REGISTERTIME_PROPERTY_KEY_STR, DataType(TIMESTAMP),
            REGISTERTIME_PROPERTY_KEY_ID, PERSON_TABLE_ID);
        descriptionProperty = Property(DESCRIPTION_PROPERTY_KEY_STR, DataType(STRING),
            DESCRIPTION_PROPERTY_KEY_ID, KNOWS_TABLE_ID);
        knowsDateProperty = Property(
            KNOWSDATE_PROPERTY_KEY_STR, DataType(DATE), KNOWSDATE_PROPERTY_KEY_ID, KNOWS_TABLE_ID);
    }

    vector<unordered_set<table_id_t>> srcNodeIDToRelIDs, dstNodeIDToRelIDs;
    Property ageProperty, nameProperty, descriptionProperty, birthDateProperty,
        registerTimeProperty, knowsDateProperty;
};

class TinySnbCatalog : public Catalog {
public:
    void setUp() {
        auto catalogContent = make_unique<NiceMock<TinySnbCatalogContent>>();
        catalogContent->setUp();
        catalogContentForReadOnlyTrx = move(catalogContent);
    }
};
