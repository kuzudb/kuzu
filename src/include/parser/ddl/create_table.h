#pragma once

#include "create_table_info.h"
#include "parser/scan_source.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CreateTable final : public Statement {
public:
    explicit CreateTable(CreateTableInfo info)
        : Statement{common::StatementType::CREATE_TABLE}, info{std::move(info)} {}

    CreateTable(CreateTableInfo info, std::unique_ptr<QueryScanSource>&& source)
        : Statement{common::StatementType::CREATE_TABLE}, info{std::move(info)},
          source{std::move(source)} {}

    inline const CreateTableInfo* getInfo() const { return &info; }
    const QueryScanSource* getSource() const { return source.get(); }

private:
    CreateTableInfo info;
    std::unique_ptr<QueryScanSource> source;
};

} // namespace parser
} // namespace kuzu
