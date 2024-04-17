#pragma once

#include "binder/bound_statement.h"
#include "parser/parsed_data/attach_info.h"

namespace kuzu {
namespace binder {

class BoundAttachDatabase final : public BoundStatement {
public:
    explicit BoundAttachDatabase(parser::AttachInfo attachInfo)
        : BoundStatement{common::StatementType::ATTACH_DATABASE,
              BoundStatementResult::createSingleStringColumnResult()},
          attachInfo{std::move(attachInfo)} {}

    parser::AttachInfo getAttachInfo() const { return attachInfo; }

private:
    parser::AttachInfo attachInfo;
};

} // namespace binder
} // namespace kuzu
