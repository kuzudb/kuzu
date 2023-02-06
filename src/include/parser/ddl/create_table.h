#pragma once

#include <vector>

#include "parser/ddl/ddl.h"

namespace kuzu {
namespace parser {

class CreateTable : public DDL {
public:
    explicit CreateTable(common::StatementType statementType, std::string tableName,
        std::vector<std::pair<std::string, std::string>> propertyNameDataTypes)
        : DDL{statementType, std::move(tableName)}, propertyNameDataTypes{
                                                        std::move(propertyNameDataTypes)} {}

    inline std::vector<std::pair<std::string, std::string>> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

protected:
    std::vector<std::pair<std::string, std::string>> propertyNameDataTypes;
};

} // namespace parser
} // namespace kuzu
