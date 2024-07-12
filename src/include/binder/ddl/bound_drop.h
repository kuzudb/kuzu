#pragma once

#include "binder/bound_statement.h"
#include "parser/ddl/drop_info.h"

namespace kuzu {
namespace binder {

class BoundDrop final : public BoundStatement {
    static constexpr common::StatementType type_ = common::StatementType::DROP;

public:
    explicit BoundDrop(const parser::DropInfo& dropInfo)
        : BoundStatement{type_, BoundStatementResult::createSingleStringColumnResult()},
          dropInfo{std::move(dropInfo)} {}

    const parser::DropInfo& getDropInfo() const { return dropInfo; };

private:
    parser::DropInfo dropInfo;
};

} // namespace binder
} // namespace kuzu
