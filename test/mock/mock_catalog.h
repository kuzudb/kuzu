#include "gmock/gmock.h"

#include "src/storage/include/catalog.h"

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
        ON_CALL(*this, containNodeProperty(0, _)).WillByDefault(Return(false));
        ON_CALL(*this, containNodeProperty(0, "age")).WillByDefault(Return(true));
        ON_CALL(*this, containNodeProperty(0, "name")).WillByDefault(Return(true));
        ON_CALL(*this, containNodeProperty(1, _)).WillByDefault(Return(false));
    }

    void setActionForContainRelProperty() {
        ON_CALL(*this, containRelProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containRelProperty(0, _)).WillByDefault(Return(false));
        ON_CALL(*this, containRelProperty(0, "description")).WillByDefault(Return(true));
        ON_CALL(*this, containRelProperty(1, _)).WillByDefault(Return(false));
    }

    void secActionForGetNodeProperty() {
        ON_CALL(*this, getNodeProperty(0, "age")).WillByDefault(ReturnRef(*ageProperty));
        ON_CALL(*this, getNodeProperty(0, "name")).WillByDefault(ReturnRef(*nameProperty));
    }

    void secActionForGetRelProperty() {
        ON_CALL(*this, getRelProperty(0, "description"))
            .WillByDefault(ReturnRef(*descriptionProperty));
    }

    void setActionForGetRelLabelsForNodeLabelDirection() {
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(0, FWD))
            .WillByDefault(ReturnRef(srcNodeLabelToRelLabels[0]));
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(0, BWD))
            .WillByDefault(ReturnRef(dstNodeLabelToRelLabels[0]));
    }

    void setActionForIsSingleMultiplicityInDirection() {
        ON_CALL(*this, isSingleMultiplicityInDirection(_, _)).WillByDefault(Return(false));
        ON_CALL(*this, isSingleMultiplicityInDirection(1, FWD)).WillByDefault(Return(true));
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
        ON_CALL(*this, getNodeLabelFromString(StrEq("person"))).WillByDefault(Return(0));
        ON_CALL(*this, getNodeLabelFromString(StrEq("organisation"))).WillByDefault(Return(1));
    }

    void setActionForGetRelLabelFromString() {
        ON_CALL(*this, getRelLabelFromString(StrEq("knows"))).WillByDefault(Return(0));
        ON_CALL(*this, getRelLabelFromString(StrEq("workAt"))).WillByDefault(Return(1));
    }

    void setSrcNodeLabelToRelLabels() {
        unordered_set<label_t> personToRelLabels = {0, 1};
        unordered_set<label_t> organisationToRelLabels = {};
        srcNodeLabelToRelLabels.push_back(move(personToRelLabels));
        srcNodeLabelToRelLabels.push_back(move(organisationToRelLabels));
    }

    void setDstNodeLabelToRelLabels() {
        unordered_set<label_t> personToRelLabels = {0};
        unordered_set<label_t> organisationToRelLabels = {1};
        dstNodeLabelToRelLabels.push_back(move(personToRelLabels));
        dstNodeLabelToRelLabels.push_back(move(organisationToRelLabels));
    }

    void setProperties() {
        ageProperty = make_unique<PropertyDefinition>("age", 0, INT32);
        nameProperty = make_unique<PropertyDefinition>("name", 1, STRING);
        descriptionProperty = make_unique<PropertyDefinition>("description", 0, STRING);
    }

    vector<unordered_set<label_t>> srcNodeLabelToRelLabels, dstNodeLabelToRelLabels;
    unique_ptr<PropertyDefinition> ageProperty, nameProperty, descriptionProperty;
};
