#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class CreateTable : public DDL {
public:
    CreateTable(PhysicalOperatorType operatorType, catalog::Catalog* catalog, std::string tableName,
        std::vector<catalog::PropertyNameDataType> propertyNameDataTypes, const DataPos& outputPos,
        uint32_t id, const std::string& paramsString)
        : DDL{operatorType, catalog, outputPos, id, paramsString}, tableName{std::move(tableName)},
          propertyNameDataTypes{std::move(propertyNameDataTypes)} {}
    ~CreateTable() override = default;

protected:
    std::string tableName;
    std::vector<catalog::PropertyNameDataType> propertyNameDataTypes;
};

} // namespace processor
} // namespace kuzu
