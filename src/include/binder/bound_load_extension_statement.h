#pragma once

#include "bound_statement.h"

namespace kuzu {
namespace binder {

class BoundLoadExtensionStatement final : public BoundStatement {
public:
    explicit BoundLoadExtensionStatement(std::string path)
        : BoundStatement{common::StatementType::LOAD_EXTENSION,
              BoundStatementResult::createEmptyResult()},
          path{std::move(path)} {}

    inline std::string getPath() const { return path; }

private:
    std::string path;
};

} // namespace binder
} // namespace kuzu
