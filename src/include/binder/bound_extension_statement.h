#pragma once

#include "bound_statement.h"
#include "extension/extension_action.h"

namespace kuzu {
namespace binder {

using namespace kuzu::extension;

class BoundExtensionStatement final : public BoundStatement {
public:
    explicit BoundExtensionStatement(std::unique_ptr<ExtensionAuxInfo> info)
        : BoundStatement{common::StatementType::EXTENSION,
              BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)} {}

    std::unique_ptr<ExtensionAuxInfo> getAuxInfo() const { return info->copy(); }

private:
    std::unique_ptr<ExtensionAuxInfo> info;
};

} // namespace binder
} // namespace kuzu
