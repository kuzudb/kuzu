#pragma once

#include "parser/ddl/ddl.h"

namespace kuzu {
namespace parser {

class DropProperty : public DDL {
public:
    explicit DropProperty(std::string tableName, std::string propertyName)
        : DDL{common::StatementType::DROP_PROPERTY, std::move(tableName)}, propertyName{std::move(
                                                                               propertyName)} {}

    inline std::string getPropertyName() const { return propertyName; };

private:
    std::string propertyName;
};

} // namespace parser
} // namespace kuzu
