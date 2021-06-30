#include "gmock/gmock.h"

#include "src/storage/include/catalog.h"

static const label_t PERSON_LABEL_ID = 0;
static const label_t ORGANISATION_LABEL_ID = 1;

static const label_t KNOWS_LABEL_ID = 0;
static const label_t WORKAT_LABEL_ID = 1;

static const char* AGE_PROPERTY_KEY_STR = "age";
static const uint32_t AGE_PROPERTY_KEY_ID = 0;
static const char* NAME_PROPERTY_KEY_STR = "name";
static const uint32_t NAME_PROPERTY_KEY_ID = 1;
static const char* BIRTHDATE_PROPERTY_KEY_STR = "birthdate";
static const uint32_t BIRTHDATE_PROPERTY_KEY_ID = 2;
static const char* DESCRIPTION_PROPERTY_KEY_STR = "description";
static const uint32_t DESCRIPTION_PROPERTY_KEY_ID = 0;
static const char* KNOWSDATE_PROPERTY_KEY_STR = "knowsdate";
static const uint32_t KNOWSDATE_PROPERTY_KEY_ID = 1;

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::StrEq;
using ::testing::Throw;

using namespace graphflow::storage;

class MockCatalog : public Catalog {

public:
    MOCK_METHOD(bool, containNodeProperty, (label_t labelId, const string& propertyName),
        (const, override));
    MOCK_METHOD(
        bool, containRelProperty, (label_t labelId, const string& propertyName), (const, override));
    MOCK_METHOD(const PropertyDefinition&, getNodeProperty,
        (label_t labelId, const string& propertyName), (const, override));
    MOCK_METHOD(const PropertyDefinition&, getRelProperty,
        (label_t labelId, const string& propertyName), (const, override));
    MOCK_METHOD(const unordered_set<label_t>&, getRelLabelsForNodeLabelDirection,
        (label_t nodeLabel, Direction direction), (const, override));
    MOCK_METHOD(bool, isSingleMultiplicityInDirection, (label_t relLabel, Direction direction),
        (const, override));
    MOCK_METHOD(bool, containNodeLabel, (const string& label), (const, override));
    MOCK_METHOD(bool, containRelLabel, (const string& label), (const, override));
    MOCK_METHOD(label_t, getRelLabelFromString, (const char* label), (const, override));
    MOCK_METHOD(label_t, getNodeLabelFromString, (const char* label), (const, override));
};

/**
 * Mock tiny snb catalog with 2 node labels person and organisation
 * and 2 rel label knows and workAt.
 * Person has property age with type INT32.
 * Knows has property description with type STRING.
 */
class TinySnbCatalog : public MockCatalog {

public:
    void setUp() {
        setSrcNodeLabelToRelLabels();
        setDstNodeLabelToRelLabels();
        setProperties();

        setActionForContainNodeProperty();
        setActionForContainRelProperty();
        secActionForGetNodeProperty();
        secActionForGetRelProperty();
        setActionForGetRelLabelsForNodeLabelDirection();
        setActionForIsSingleMultiplicityInDirection();
        setActionForContainNodeLabel();
        setActionForContainRelLabel();
        setActionForGetNodeLabelFromString();
        setActionForGetRelLabelFromString();
    }

private:
    void setActionForContainNodeProperty() {
        ON_CALL(*this, containNodeProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containNodeProperty(PERSON_LABEL_ID, _)).WillByDefault(Return(false));
        ON_CALL(*this, containNodeProperty(PERSON_LABEL_ID, AGE_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containNodeProperty(PERSON_LABEL_ID, NAME_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containNodeProperty(PERSON_LABEL_ID, BIRTHDATE_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containNodeProperty(ORGANISATION_LABEL_ID, _)).WillByDefault(Return(false));
    }

    void setActionForContainRelProperty() {
        ON_CALL(*this, containRelProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containRelProperty(KNOWS_LABEL_ID, _)).WillByDefault(Return(false));
        ON_CALL(*this, containRelProperty(KNOWS_LABEL_ID, DESCRIPTION_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containRelProperty(KNOWS_LABEL_ID, KNOWSDATE_PROPERTY_KEY_STR))
            .WillByDefault(Return(true));
        ON_CALL(*this, containRelProperty(1, _)).WillByDefault(Return(false));
    }

    void secActionForGetNodeProperty() {
        ON_CALL(*this, getNodeProperty(PERSON_LABEL_ID, AGE_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(*ageProperty));
        ON_CALL(*this, getNodeProperty(PERSON_LABEL_ID, NAME_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(*nameProperty));
        ON_CALL(*this, getNodeProperty(PERSON_LABEL_ID, BIRTHDATE_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(*birthDateProperty));
    }

    void secActionForGetRelProperty() {
        ON_CALL(*this, getRelProperty(KNOWS_LABEL_ID, DESCRIPTION_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(*descriptionProperty));
        ON_CALL(*this, getRelProperty(KNOWS_LABEL_ID, KNOWSDATE_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(*knowsDateProperty));
    }

    void setActionForGetRelLabelsForNodeLabelDirection() {
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(PERSON_LABEL_ID, FWD))
            .WillByDefault(ReturnRef(srcNodeLabelToRelLabels[PERSON_LABEL_ID]));
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(PERSON_LABEL_ID, BWD))
            .WillByDefault(ReturnRef(dstNodeLabelToRelLabels[PERSON_LABEL_ID]));
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(ORGANISATION_LABEL_ID, FWD))
            .WillByDefault(ReturnRef(srcNodeLabelToRelLabels[ORGANISATION_LABEL_ID]));
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(ORGANISATION_LABEL_ID, BWD))
            .WillByDefault(ReturnRef(dstNodeLabelToRelLabels[ORGANISATION_LABEL_ID]));
    }

    void setActionForIsSingleMultiplicityInDirection() {
        ON_CALL(*this, isSingleMultiplicityInDirection(_, _)).WillByDefault(Return(false));
        ON_CALL(*this, isSingleMultiplicityInDirection(WORKAT_LABEL_ID, FWD))
            .WillByDefault(Return(true));
    }

    void setActionForContainNodeLabel() {
        ON_CALL(*this, containNodeLabel(_)).WillByDefault(Return(false));
        ON_CALL(*this, containNodeLabel(StrEq("person"))).WillByDefault(Return(true));
        ON_CALL(*this, containNodeLabel(StrEq("organisation"))).WillByDefault(Return(true));
    }

    void setActionForContainRelLabel() {
        ON_CALL(*this, containRelLabel(_)).WillByDefault(Return(false));
        ON_CALL(*this, containRelLabel(StrEq("knows"))).WillByDefault(Return(true));
        ON_CALL(*this, containRelLabel(StrEq("workAt"))).WillByDefault(Return(true));
    }

    void setActionForGetNodeLabelFromString() {
        ON_CALL(*this, getNodeLabelFromString(StrEq("person")))
            .WillByDefault(Return(PERSON_LABEL_ID));
        ON_CALL(*this, getNodeLabelFromString(StrEq("organisation")))
            .WillByDefault(Return(ORGANISATION_LABEL_ID));
    }

    void setActionForGetRelLabelFromString() {
        ON_CALL(*this, getRelLabelFromString(StrEq("knows"))).WillByDefault(Return(KNOWS_LABEL_ID));
        ON_CALL(*this, getRelLabelFromString(StrEq("workAt")))
            .WillByDefault(Return(WORKAT_LABEL_ID));
    }

    void setSrcNodeLabelToRelLabels() {
        unordered_set<label_t> personToRelLabels = {KNOWS_LABEL_ID, WORKAT_LABEL_ID};
        unordered_set<label_t> organisationToRelLabels = {};
        srcNodeLabelToRelLabels.push_back(move(personToRelLabels));
        srcNodeLabelToRelLabels.push_back(move(organisationToRelLabels));
    }

    void setDstNodeLabelToRelLabels() {
        unordered_set<label_t> personToRelLabels = {KNOWS_LABEL_ID};
        unordered_set<label_t> organisationToRelLabels = {WORKAT_LABEL_ID};
        dstNodeLabelToRelLabels.push_back(move(personToRelLabels));
        dstNodeLabelToRelLabels.push_back(move(organisationToRelLabels));
    }

    void setProperties() {
        ageProperty =
            make_unique<PropertyDefinition>(AGE_PROPERTY_KEY_STR, AGE_PROPERTY_KEY_ID, INT32);
        nameProperty =
            make_unique<PropertyDefinition>(NAME_PROPERTY_KEY_STR, NAME_PROPERTY_KEY_ID, STRING);
        birthDateProperty = make_unique<PropertyDefinition>(
            BIRTHDATE_PROPERTY_KEY_STR, BIRTHDATE_PROPERTY_KEY_ID, DATE);
        descriptionProperty = make_unique<PropertyDefinition>(
            DESCRIPTION_PROPERTY_KEY_STR, DESCRIPTION_PROPERTY_KEY_ID, STRING);
        knowsDateProperty = make_unique<PropertyDefinition>(
            KNOWSDATE_PROPERTY_KEY_STR, KNOWSDATE_PROPERTY_KEY_ID, DATE);
    }

    vector<unordered_set<label_t>> srcNodeLabelToRelLabels, dstNodeLabelToRelLabels;
    unique_ptr<PropertyDefinition> ageProperty, nameProperty, descriptionProperty,
        birthDateProperty, knowsDateProperty;
};
