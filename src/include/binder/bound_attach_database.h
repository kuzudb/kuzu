#pragma once

#include "binder/bound_statement.h"
#include "bound_attach_info.h"

namespace kuzu {
namespace binder {

class BoundAttachDatabase final : public BoundStatement {
public:
    explicit BoundAttachDatabase(binder::AttachInfo attachInfo)
        : BoundStatement{common::StatementType::ATTACH_DATABASE,
              BoundStatementResult::createSingleStringColumnResult()},
          attachInfo{std::move(attachInfo)} {}

    binder::AttachInfo getAttachInfo() const { return attachInfo; }

private:
    binder::AttachInfo attachInfo;
};

} // namespace binder
} // namespace kuzu
