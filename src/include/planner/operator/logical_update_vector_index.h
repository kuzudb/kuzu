#pragma once

#include "binder/expression/expression.h"
#include "common/vector_index/vector_index_config.h"
#include "planner/operator/logical_operator.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

class LogicalUpdateVectorIndex : public LogicalOperator {
public:
    LogicalUpdateVectorIndex(std::string tableName, std::string propertyName, table_id_t tableId,
        property_id_t propertyId, std::shared_ptr<Expression> offset,
        std::shared_ptr<Expression> embeddingProperty, binder::expression_vector outExprs,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::UPDATE_VECTOR_INDEX, std::move(child)},
          tableName{std::move(tableName)}, propertyName{std::move(propertyName)}, tableId{tableId},
          propertyId{propertyId}, offset{std::move(offset)},
          embeddingProperty{std::move(embeddingProperty)}, outExprs{std::move(outExprs)} {}

    std::string getExpressionsForPrinting() const override {
        return stringFormat("Indexing_{}_{}", tableName, propertyName);
    }

    void computeFactorizedSchema() override;

    void computeFlatSchema() override;

    inline table_id_t getTableId() const { return tableId; }
    inline property_id_t getPropertyId() const { return propertyId; }
    inline std::shared_ptr<Expression> getOffset() const { return offset; }
    inline std::shared_ptr<Expression> getEmbeddingProperty() const { return embeddingProperty; }
    inline const expression_vector& getOutExprs() const { return outExprs; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalUpdateVectorIndex>(tableName, propertyName, tableId, propertyId,
            offset, embeddingProperty, outExprs, children[0]->copy());
    }

private:
    std::string tableName;
    std::string propertyName;
    table_id_t tableId;
    property_id_t propertyId;
    std::shared_ptr<binder::Expression> offset;
    std::shared_ptr<binder::Expression> embeddingProperty;
    binder::expression_vector outExprs;
};

} // namespace planner
} // namespace kuzu
