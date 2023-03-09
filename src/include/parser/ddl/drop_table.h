#pragma once

#include "parser/ddl/ddl.h"

namespace kuzu {
namespace parser {

class DropTable : public DDL {
public:
    explicit DropTable(std::string tableName)
        : DDL{common::StatementType::DROP_TABLE, std::move(tableName)} {}
};

} // namespace parser
} // namespace kuzu
