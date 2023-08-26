#pragma once

#include "planner/logical_plan/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateRDFGraph : public LogicalDDL {
public:
    explicit LogicalCreateRDFGraph(
        std::string graphName, std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::CREATE_RDF_GRAPH, std::move(graphName),
              std::move(outputExpression)} {}

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRDFGraph>(tableName, outputExpression);
    }
};

} // namespace planner
} // namespace kuzu
