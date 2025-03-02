#pragma once

#include "binder/bound_statement.h"
#include "bound_create_sequence_info.h"
namespace kuzu {
namespace binder {

class BoundCreateSequence final : public BoundStatement {
public:
    explicit BoundCreateSequence(BoundCreateSequenceInfo info)
        : BoundStatement{common::StatementType::CREATE_SEQUENCE,
              BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)} {}

    const BoundCreateSequenceInfo* getInfo() const { return &info; }

private:
    BoundCreateSequenceInfo info;
};

} // namespace binder
} // namespace kuzu
