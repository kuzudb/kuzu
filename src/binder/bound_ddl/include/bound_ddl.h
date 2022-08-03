#pragma once

#include "src/binder/bound_statement/include/bound_statement.h"
#include "src/catalog/include/catalog_structs.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace binder {

class BoundDDL : public BoundStatement {
public:
    explicit BoundDDL(StatementType statementType, string labelName,
        vector<PropertyNameDataType> propertyNameDataTypes)
        : BoundStatement{statementType}, labelName{move(labelName)}, propertyNameDataTypes{move(
                                                                         propertyNameDataTypes)} {}

    inline string getLabelName() const { return labelName; }
    inline vector<PropertyNameDataType> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

private:
    string labelName;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace binder
} // namespace graphflow
