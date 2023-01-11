#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class CreateTable : public DDL {
public:
    CreateTable(PhysicalOperatorType operatorType, Catalog* catalog, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, const DataPos& outputPos, uint32_t id,
        const string& paramsString)
        : DDL{operatorType, catalog, outputPos, id, paramsString}, tableName{std::move(tableName)},
          propertyNameDataTypes{std::move(propertyNameDataTypes)} {}
    virtual ~CreateTable() override = default;

protected:
    string tableName;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace processor
} // namespace kuzu
