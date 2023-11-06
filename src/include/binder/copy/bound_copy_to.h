#pragma once

#include "binder/query/bound_regular_query.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

class BoundCopyTo : public BoundStatement {
public:
    BoundCopyTo(std::unique_ptr<common::ReaderConfig> config,
        std::unique_ptr<BoundRegularQuery> regularQuery)
        : BoundStatement{common::StatementType::COPY_TO, BoundStatementResult::createEmptyResult()},
          config{std::move(config)}, regularQuery{std::move(regularQuery)} {}

    inline common::ReaderConfig* getConfig() const { return config.get(); }

    inline BoundRegularQuery* getRegularQuery() const { return regularQuery.get(); }

private:
    std::unique_ptr<common::ReaderConfig> config;
    std::unique_ptr<BoundRegularQuery> regularQuery;
};

} // namespace binder
} // namespace kuzu
