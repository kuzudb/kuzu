#pragma once

#include "src/catalog/include/catalog_structs.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace binder {

class BoundCreateNodeClause {
public:
    explicit BoundCreateNodeClause(
        string labelName, string primaryKey, vector<PropertyNameDataType> propertyNameDataTypes)
        : labelName{move(labelName)}, primaryKey{move(primaryKey)}, propertyNameDataTypes{move(
                                                                        propertyNameDataTypes)} {}

    inline string getLabelName() { return labelName; }
    inline string getPrimaryKey() { return primaryKey; }
    inline vector<PropertyNameDataType> getPropertyNameDataTypes() { return propertyNameDataTypes; }

private:
    string labelName;
    string primaryKey;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace binder
} // namespace graphflow
