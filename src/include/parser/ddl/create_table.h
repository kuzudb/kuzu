#pragma once

#include "create_table_info.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CreateTable final : public Statement {
public:
    explicit CreateTable(CreateTableInfo info)
        : Statement{common::StatementType::CREATE_TABLE}, info{std::move(info)} {}
    
    CreateTable(CreateTableInfo info, std::unique_ptr<Statement>&& innerQuery)
        : Statement{common::StatementType::CREATE_TABLE}, info{std::move(info)}, innerQuery{std::move(innerQuery)} {}

    inline const CreateTableInfo* getInfo() const { return &info; }
    
    bool hasInnerQuery() const { return innerQuery.has_value(); }
    const Statement* getInnerQuery() const { return innerQuery.value().get(); }

private:
    CreateTableInfo info;
    std::optional<std::unique_ptr<Statement>> innerQuery;
};

} // namespace parser
} // namespace kuzu
