#pragma once

#include "create_table_info.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CreateTable final : public Statement {
public:
    explicit CreateTable(CreateTableInfo info)
        : Statement{common::StatementType::CREATE_TABLE}, info{std::move(info)} {}

    inline const CreateTableInfo* getInfo() const { return &info; }

private:
    CreateTableInfo info;
};

} // namespace parser
} // namespace kuzu
