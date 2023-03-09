#pragma once

#include "parser/ddl/ddl.h"

namespace kuzu {
namespace parser {

class RenameProperty : public DDL {
public:
    explicit RenameProperty(std::string tableName, std::string propertyName, std::string newName)
        : DDL{common::StatementType::RENAME_PROPERTY, std::move(tableName)},
          propertyName{std::move(propertyName)}, newName{std::move(newName)} {}

    inline std::string getPropertyName() const { return propertyName; }

    inline std::string getNewName() const { return newName; }

private:
    std::string propertyName;
    std::string newName;
};

} // namespace parser
} // namespace kuzu
