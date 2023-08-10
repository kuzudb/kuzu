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
    BoundCopyTo(
        common::CopyDescription copyDescription, std::unique_ptr<BoundRegularQuery> regularQuery)
        : BoundStatement{common::StatementType::COPY_TO, BoundStatementResult::createEmptyResult()},
          regularQuery{std::move(regularQuery)}, copyDescription{std::move(copyDescription)} {}

    inline common::CopyDescription getCopyDescription() const { return copyDescription; }

    inline BoundRegularQuery* getRegularQuery() const { return regularQuery.get(); }

private:
    common::CopyDescription copyDescription;
    std::unique_ptr<BoundRegularQuery> regularQuery;
};

} // namespace binder
} // namespace kuzu
