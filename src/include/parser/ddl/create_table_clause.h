#pragma once

#include "create_table_info.h"
#include "ddl.h"

namespace kuzu {
namespace parser {

class CreateTableClause : public DDL {
public:
    CreateTableClause(common::StatementType statementType, std::string tableName,
        std::unique_ptr<CreateTableInfo> info)
        : DDL{statementType, std::move(tableName)}, info{std::move(info)} {}

    inline CreateTableInfo* getInfo() const { return info.get(); }

private:
    std::unique_ptr<CreateTableInfo> info;
};

} // namespace parser
} // namespace kuzu
