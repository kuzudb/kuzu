#pragma once

#include "create_table_info.h"
#include "ddl.h"

namespace kuzu {
namespace parser {

class CreateTable : public DDL {
public:
    CreateTable(std::string tableName, std::unique_ptr<CreateTableInfo> info)
        : DDL{common::StatementType::CREATE_TABLE, std::move(tableName)}, info{std::move(info)} {}

    inline CreateTableInfo* getInfo() const { return info.get(); }

private:
    std::unique_ptr<CreateTableInfo> info;
};

} // namespace parser
} // namespace kuzu
