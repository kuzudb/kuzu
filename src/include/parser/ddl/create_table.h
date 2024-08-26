#pragma once

#include "create_table_info.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CreateTable final : public Statement {
    static constexpr common::StatementType statementType_ = common::StatementType::CREATE_TABLE;

public:
    explicit CreateTable(CreateTableInfo info) : Statement{statementType_}, info{std::move(info)} {}

    const CreateTableInfo& getInfo() const { return info; }

private:
    CreateTableInfo info;
};

} // namespace parser
} // namespace kuzu
