#pragma once

#include "parser/ddl/ddl.h"

namespace kuzu {
namespace parser {

using namespace std;

class RenameProperty : public DDL {
public:
    explicit RenameProperty(string tableName, string propertyName, string newName)
        : DDL{StatementType::RENAME_PROPERTY, std::move(tableName)},
          propertyName{std::move(propertyName)}, newName{std::move(newName)} {}

    inline string getPropertyName() const { return propertyName; }

    inline string getNewName() const { return newName; }

private:
    string propertyName;
    string newName;
};

} // namespace parser
} // namespace kuzu
