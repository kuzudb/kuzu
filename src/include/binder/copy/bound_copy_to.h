#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "binder/query/bound_regular_query.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

class BoundCopyTo : public BoundStatement {
public:
    BoundCopyTo(std::unique_ptr<common::CopyDescription> copyDescription,
        std::unique_ptr<BoundRegularQuery> regularQuery)
        : BoundStatement{common::StatementType::COPY_TO, BoundStatementResult::createEmptyResult()},
          copyDescription{std::move(copyDescription)}, regularQuery{std::move(regularQuery)} {}

    inline common::CopyDescription* getCopyDescription() const { return copyDescription.get(); }

    inline BoundRegularQuery* getRegularQuery() const { return regularQuery.get(); }

private:
    std::unique_ptr<common::CopyDescription> copyDescription;
    std::unique_ptr<BoundRegularQuery> regularQuery;
};

} // namespace binder
} // namespace kuzu
