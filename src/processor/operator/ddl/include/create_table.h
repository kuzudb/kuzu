#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class CreateTable : public DDL {

public:
    CreateTable(Catalog* catalog, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, uint32_t id, const string& paramsString)
        : DDL{catalog, id, paramsString}, tableName{move(tableName)}, propertyNameDataTypes{move(
                                                                          propertyNameDataTypes)} {}

    virtual ~CreateTable() = default;

protected:
    string tableName;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace processor
} // namespace kuzu
