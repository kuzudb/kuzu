#include "gmock/gmock.h"

#include "src/catalog/include/catalog.h"

static const char* PERSON_LABEL_STR = "person";
static const label_t PERSON_LABEL_ID = 0;
static const char* ORGANISATION_LABEL_STR = "organisation";
static const label_t ORGANISATION_LABEL_ID = 1;

static const char* KNOWS_LABEL_STR = "knows";
static const label_t KNOWS_LABEL_ID = 0;
static const char* WORKAT_LABEL_STR = "workAt";
static const label_t WORKAT_LABEL_ID = 1;

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
    MOCK_METHOD(bool, containNodeProperty, (label_t labelId, const string& propertyName),
        (const, override));
    MOCK_METHOD(
        bool, containRelProperty, (label_t labelId, const string& propertyName), (const, override));
    MOCK_METHOD(const Property&, getNodeProperty, (label_t labelId, const string& propertyName),
        (const, override));
    MOCK_METHOD(const Property&, getRelProperty, (label_t labelId, const string& propertyName),
        (const, override));
    MOCK_METHOD(const unordered_set<label_t>&, getRelLabelsForNodeLabelDirection,
        (label_t nodeLabel, RelDirection direction), (const, override));
    MOCK_METHOD(bool, isSingleMultiplicityInDirection, (label_t relLabel, RelDirection direction),
        (const, override));
    MOCK_METHOD(bool, containNodeLabel, (const string& label), (const, override));
    MOCK_METHOD(bool, containRelLabel, (const string& label), (const, override));
    MOCK_METHOD(label_t, getRelLabelFromName, (const string& label), (const, override));
    MOCK_METHOD(label_t, getNodeLabelFromName, (const string& label), (const, override));
    MOCK_METHOD(string, getNodeLabelName, (label_t labelId), (const, override));
    MOCK_METHOD(string, getRelLabelName, (label_t labelId), (const, override));
    MOCK_METHOD(uint64_t, getNumNodes, (label_t labelId), (const override));
    MOCK_METHOD(uint64_t, getNumRelsForDirectionBoundLabel,
        (label_t relLabelId, RelDirection relDirection, label_t nodeLabelId), (const override));
};

/**
 * Mock tiny snb catalogContent with 2 node labels person and organisation
 * and 2 rel label knows and workAt.
 * Person has property age with type INT64.
 * Knows has property description with type STRING.
 */
class TinySnbCatalogContent : public MockCatalogContent {

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
        setActionForGetNodeLabelName();
        setActionForGetRelLabelName();
        setActionForgetNumRelsForDirectionBoundLabel();
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
        ON_CALL(*this, containNodeProperty(PERSON_LABEL_ID, REGISTERTIME_PROPERTY_KEY_STR))
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
            .WillByDefault(ReturnRef(ageProperty));
        ON_CALL(*this, getNodeProperty(PERSON_LABEL_ID, NAME_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(nameProperty));
        ON_CALL(*this, getNodeProperty(PERSON_LABEL_ID, BIRTHDATE_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(birthDateProperty));
        ON_CALL(*this, getNodeProperty(PERSON_LABEL_ID, REGISTERTIME_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(registerTimeProperty));
    }

    void secActionForGetRelProperty() {
        ON_CALL(*this, getRelProperty(KNOWS_LABEL_ID, DESCRIPTION_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(descriptionProperty));
        ON_CALL(*this, getRelProperty(KNOWS_LABEL_ID, KNOWSDATE_PROPERTY_KEY_STR))
            .WillByDefault(ReturnRef(knowsDateProperty));
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
        ON_CALL(*this, containNodeLabel(StrEq(PERSON_LABEL_STR))).WillByDefault(Return(true));
        ON_CALL(*this, containNodeLabel(StrEq(ORGANISATION_LABEL_STR))).WillByDefault(Return(true));
    }

    void setActionForContainRelLabel() {
        ON_CALL(*this, containRelLabel(_)).WillByDefault(Return(false));
        ON_CALL(*this, containRelLabel(StrEq(KNOWS_LABEL_STR))).WillByDefault(Return(true));
        ON_CALL(*this, containRelLabel(StrEq(WORKAT_LABEL_STR))).WillByDefault(Return(true));
    }

    void setActionForGetNodeLabelFromString() {
        ON_CALL(*this, getNodeLabelFromName(StrEq(PERSON_LABEL_STR)))
            .WillByDefault(Return(PERSON_LABEL_ID));
        ON_CALL(*this, getNodeLabelFromName(StrEq(ORGANISATION_LABEL_STR)))
            .WillByDefault(Return(ORGANISATION_LABEL_ID));
    }

    void setActionForGetRelLabelFromString() {
        ON_CALL(*this, getRelLabelFromName(StrEq(KNOWS_LABEL_STR)))
            .WillByDefault(Return(KNOWS_LABEL_ID));
        ON_CALL(*this, getRelLabelFromName(StrEq(WORKAT_LABEL_STR)))
            .WillByDefault(Return(WORKAT_LABEL_ID));
    }

    void setActionForGetNodeLabelName() {
        ON_CALL(*this, getNodeLabelName(PERSON_LABEL_ID)).WillByDefault(Return(PERSON_LABEL_STR));
        ON_CALL(*this, getNodeLabelName(ORGANISATION_LABEL_ID))
            .WillByDefault(Return(ORGANISATION_LABEL_STR));
    }

    void setActionForGetRelLabelName() {
        ON_CALL(*this, getRelLabelName(KNOWS_LABEL_ID)).WillByDefault(Return(KNOWS_LABEL_STR));
        ON_CALL(*this, getRelLabelName(WORKAT_LABEL_ID)).WillByDefault(Return(WORKAT_LABEL_STR));
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
        PropertyNameDataType agePropertyDefinition(AGE_PROPERTY_KEY_STR, INT64);
        ageProperty = Property::constructStructuredNodeProperty(
            agePropertyDefinition, AGE_PROPERTY_KEY_ID, PERSON_LABEL_ID);
        PropertyNameDataType namePropertyDefinition(NAME_PROPERTY_KEY_STR, STRING);
        nameProperty = Property::constructStructuredNodeProperty(
            namePropertyDefinition, NAME_PROPERTY_KEY_ID, PERSON_LABEL_ID);
        PropertyNameDataType birthDatePropertyDefinition(BIRTHDATE_PROPERTY_KEY_STR, DATE);
        birthDateProperty = Property::constructStructuredNodeProperty(
            birthDatePropertyDefinition, BIRTHDATE_PROPERTY_KEY_ID, PERSON_LABEL_ID);
        PropertyNameDataType registerTimePropertyDefinition(
            REGISTERTIME_PROPERTY_KEY_STR, TIMESTAMP);
        registerTimeProperty = Property::constructStructuredNodeProperty(
            registerTimePropertyDefinition, REGISTERTIME_PROPERTY_KEY_ID, PERSON_LABEL_ID);
        PropertyNameDataType descriptionPropertyDefinition(DESCRIPTION_PROPERTY_KEY_STR, STRING);
        descriptionProperty = Property::constructRelProperty(
            descriptionPropertyDefinition, DESCRIPTION_PROPERTY_KEY_ID, KNOWS_LABEL_ID);
        PropertyNameDataType knowsDatePropertyDefinition(KNOWSDATE_PROPERTY_KEY_STR, DATE);
        knowsDateProperty = Property::constructRelProperty(
            knowsDatePropertyDefinition, KNOWSDATE_PROPERTY_KEY_ID, KNOWS_LABEL_ID);
    }

    void setActionForgetNumRelsForDirectionBoundLabel() {
        ON_CALL(*this, getNumRelsForDirectionBoundLabel(_, _, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getNumRelsForDirectionBoundLabel(0, FWD, 0))
            .WillByDefault(Return(10 * NUM_PERSON_NODES));
        ON_CALL(*this, getNumRelsForDirectionBoundLabel(0, BWD, 0))
            .WillByDefault(Return(20 * NUM_PERSON_NODES));
        ON_CALL(*this, getNumRelsForDirectionBoundLabel(0, FWD, 1))
            .WillByDefault(Return(1 * NUM_PERSON_NODES));
        ON_CALL(*this, getNumRelsForDirectionBoundLabel(1, BWD, 1))
            .WillByDefault(Return(100 * NUM_ORGANISATION_NODES));
    }

    vector<unordered_set<label_t>> srcNodeLabelToRelLabels, dstNodeLabelToRelLabels;
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
