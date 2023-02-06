#pragma once

#include "parser/ddl/ddl.h"

namespace kuzu {
namespace parser {

class RenameTable : public DDL {
public:
    explicit RenameTable(std::string tableName, std::string newName)
        : DDL{common::StatementType::RENAME_TABLE, std::move(tableName)}, newName{
                                                                              std::move(newName)} {}

    inline std::string getNewName() const { return newName; }

private:
    std::string newName;
};

} // namespace parser
} // namespace kuzu
