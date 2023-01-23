#pragma once

#include "parser/ddl/ddl.h"

namespace kuzu {
namespace parser {

using namespace std;

class RenameTable : public DDL {
public:
    explicit RenameTable(string tableName, string newName)
        : DDL{StatementType::RENAME_TABLE, std::move(tableName)}, newName{std::move(newName)} {}

    inline string getNewName() const { return newName; }

private:
    string newName;
};

} // namespace parser
} // namespace kuzu
