#pragma once

#include "src/binder/bound_ddl/include/bound_ddl.h"
#include "src/catalog/include/catalog_structs.h"

namespace graphflow {
namespace binder {

class BoundCreateRelClause : public BoundDDL {
public:
    explicit BoundCreateRelClause(string labelName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        vector<pair<label_t, label_t>> relConnections)
        : BoundDDL{StatementType::CREATE_REL_CLAUSE, move(labelName), move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, relConnections{move(relConnections)} {}

    RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    vector<pair<label_t, label_t>> getRelConnections() const { return relConnections; }

private:
    RelMultiplicity relMultiplicity;
    vector<pair<label_t, label_t>> relConnections;
};

} // namespace binder
} // namespace graphflow
