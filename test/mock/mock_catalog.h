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
    MOCK_METHOD(bool, containNodeLabel, (const char* label), (const, override));
    MOCK_METHOD(label_t, getNodeLabelFromString, (const char* label), (const, override));
    MOCK_METHOD(bool, containRelLabel, (const char* label), (const, override));
    MOCK_METHOD(label_t, getRelLabelFromString, (const char* label), (const, override));
    MOCK_METHOD(const vector<label_t>&, getRelLabelsForNodeLabelDirection,
        (label_t nodeLabel, Direction dir), (const, override));
    MOCK_METHOD(bool, containNodeProperty, (label_t nodeLabel, const string& propertyName),
        (const, override));
    MOCK_METHOD(DataType, getNodePropertyTypeFromString,
        (label_t nodeLabel, const string& propertyName), (const, override));
    MOCK_METHOD(uint32_t, getNodePropertyKeyFromString,
        (label_t nodeLabel, const string& propertyName), (const, override));
    MOCK_METHOD(bool, containUnstrNodeProperty, (label_t nodeLabel, const string& propertyName),
        (const, override));
    MOCK_METHOD(bool, containRelProperty, (label_t relLabel, const string& propertyName),
        (const, override));
    MOCK_METHOD(DataType, getRelPropertyTypeFromString,
        (label_t relLabel, const string& propertyName), (const, override));
    MOCK_METHOD(uint32_t, getRelPropertyKeyFromString,
        (label_t relLabel, const string& propertyName), (const, override));
    MOCK_METHOD(
        bool, isSingleCaridinalityInDir, (label_t relLabel, Direction dir), (const, override));
};

/**
 * Mock person-knows-person catalog with 1 node label person
 * and 1 rel label knows. Person has property age with type INT32.
 * Knows has property description with type STRING.
 */
class PersonKnowsPersonCatalog : public MockCatalog {

public:
    void setUp() {
        setSrcNodeLabelToRelLabels();
        setDstNodeLabelToRelLabels();

        setActionForContainNodeLabel();
        setActionForGetNodeLabelFromString();
        setActionForContainRelLabel();
        setActionForGetRelLabelFromString();
        setActionForGetRelLabelsForNodeLabelDirection();
        setActionForContainNodeProperty();
        setActionForGetNodePropertyTypeFromString();
        setActionForGetNodePropertyKeyFromString();
        setActionForContainUnstrNodeProperty();
        setActionForContainRelProperty();
        setActionForGetRelPropertyTypeFromString();
        setActionForGetRelPropertyKeyFromString();
        setActionForIsSingleCardinalityInDir();
    }

private:
    void setActionForContainNodeLabel() {
        ON_CALL(*this, containNodeLabel(_)).WillByDefault(Return(false));
        ON_CALL(*this, containNodeLabel(StrEq("person"))).WillByDefault(Return(true));
    }

    void setActionForGetNodeLabelFromString() {
        ON_CALL(*this, getNodeLabelFromString(StrEq("person"))).WillByDefault(Return(0));
    }

    void setActionForContainRelLabel() {
        ON_CALL(*this, containRelLabel(_)).WillByDefault(Return(false));
        ON_CALL(*this, containRelLabel(StrEq("knows"))).WillByDefault(Return(true));
    }

    void setActionForGetRelLabelFromString() {
        ON_CALL(*this, getRelLabelFromString(StrEq("knows"))).WillByDefault(Return(0));
    }

    void setActionForGetRelLabelsForNodeLabelDirection() {
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(0, FWD))
            .WillByDefault(ReturnRef(srcNodeLabelToRelLabels[0]));
        ON_CALL(*this, getRelLabelsForNodeLabelDirection(0, BWD))
            .WillByDefault(ReturnRef(dstNodeLabelToRelLabels[0]));
    }

    void setActionForContainNodeProperty() {
        ON_CALL(*this, containNodeProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containNodeProperty(0, _)).WillByDefault(Return(false));
        ON_CALL(*this, containNodeProperty(0, "age")).WillByDefault(Return(true));
        ON_CALL(*this, containNodeProperty(0, "name")).WillByDefault(Return(true));
    }

    void setActionForGetNodePropertyTypeFromString() {
        ON_CALL(*this, getNodePropertyTypeFromString(0, "age")).WillByDefault(Return(INT32));
        ON_CALL(*this, getNodePropertyTypeFromString(0, "name")).WillByDefault(Return(STRING));
    }

    void setActionForGetNodePropertyKeyFromString() {
        ON_CALL(*this, getNodePropertyKeyFromString(0, "age")).WillByDefault(Return(0));
        ON_CALL(*this, getNodePropertyKeyFromString(0, "name")).WillByDefault(Return(1));
    }

    void setActionForContainUnstrNodeProperty() {
        ON_CALL(*this, containUnstrNodeProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containUnstrNodeProperty(0, _)).WillByDefault(Return(false));
    }

    void setActionForContainRelProperty() {
        ON_CALL(*this, containRelProperty(_, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, containRelProperty(0, _)).WillByDefault(Return(false));
        ON_CALL(*this, containRelProperty(0, "description")).WillByDefault(Return(true));
    }

    void setActionForGetRelPropertyTypeFromString() {
        ON_CALL(*this, getRelPropertyTypeFromString(0, "description"))
            .WillByDefault(Return(STRING));
    }

    void setActionForGetRelPropertyKeyFromString() {
        ON_CALL(*this, getRelPropertyKeyFromString(0, "description")).WillByDefault(Return(0));
    }

    void setActionForIsSingleCardinalityInDir() {
        ON_CALL(*this, isSingleCaridinalityInDir(_, _)).WillByDefault(Return(false));
    }

    void setSrcNodeLabelToRelLabels() {
        vector<label_t> personToRelLabels = {0};
        srcNodeLabelToRelLabels.push_back(move(personToRelLabels));
    }

    void setDstNodeLabelToRelLabels() {
        vector<label_t> personToRelLabels = {0};
        dstNodeLabelToRelLabels.push_back(move(personToRelLabels));
    }

    vector<vector<label_t>> srcNodeLabelToRelLabels, dstNodeLabelToRelLabels;
};
