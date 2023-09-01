#pragma once

#include "catalog/table_schema.h"
#include "common/copier_config/copier_config.h"
#include "planner/logical_plan/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCopyFrom : public LogicalOperator {

public:
    LogicalCopyFrom(std::unique_ptr<common::CopyDescription> copyDescription,
        catalog::TableSchema* tableSchema, binder::expression_vector columnExpressions,
        std::shared_ptr<binder::Expression> nodeOffsetExpression,
        std::shared_ptr<binder::Expression> outputExpression, bool orderPreserving_)
        : LogicalOperator{LogicalOperatorType::COPY_FROM},
          copyDescription{std::move(copyDescription)}, tableSchema{tableSchema},
          columnExpressions{std::move(columnExpressions)}, nodeOffsetExpression{std::move(
                                                               nodeOffsetExpression)},
          outputExpression{std::move(outputExpression)}, orderPreserving_{orderPreserving_} {}

    inline std::string getExpressionsForPrinting() const override { return tableSchema->tableName; }

    inline common::CopyDescription* getCopyDescription() const { return copyDescription.get(); }
    inline catalog::TableSchema* getTableSchema() const { return tableSchema; }
    inline binder::expression_vector getColumnExpressions() const { return columnExpressions; }
    inline std::shared_ptr<binder::Expression> getNodeOffsetExpression() const {
        return nodeOffsetExpression;
    }
    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    inline bool orderPreserving() { return orderPreserving_; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyFrom>(copyDescription->copy(), tableSchema, columnExpressions,
            nodeOffsetExpression, outputExpression, orderPreserving_);
    }

private:
    std::unique_ptr<common::CopyDescription> copyDescription;
    catalog::TableSchema* tableSchema;
    binder::expression_vector columnExpressions;
    std::shared_ptr<binder::Expression> nodeOffsetExpression;
    std::shared_ptr<binder::Expression> outputExpression;
    bool orderPreserving_;
};

} // namespace planner
} // namespace kuzu
