#include "gmock/gmock.h"

#include "src/storage/include/catalog.h"

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
