#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class CreateTable : public DDL {
public:
    CreateTable(PhysicalOperatorType operatorType, catalog::Catalog* catalog, std::string tableName,
        std::vector<std::unique_ptr<catalog::Property>> properties, const DataPos& outputPos,
        uint32_t id, const std::string& paramsString)
        : DDL{operatorType, catalog, outputPos, id, paramsString}, tableName{std::move(tableName)},
          properties{std::move(properties)} {}
    ~CreateTable() override = default;

protected:
    std::string tableName;
    std::vector<std::unique_ptr<catalog::Property>> properties;
};

} // namespace processor
} // namespace kuzu
