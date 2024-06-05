#pragma once

#include "binder/bound_statement.h"

namespace kuzu {
namespace binder {

class BoundCreateType final : public BoundStatement {
public:
    explicit BoundCreateType(std::string name, common::LogicalType type)
        : BoundStatement{common::StatementType::CREATE_TYPE,
              BoundStatementResult::createSingleStringColumnResult()},
          name{std::move(name)}, type{std::move(type)} {}

    std::string getName() const { return name; };

    common::LogicalType getType() const { return type; }

private:
    std::string name;
    common::LogicalType type;
};

} // namespace binder
} // namespace kuzu
