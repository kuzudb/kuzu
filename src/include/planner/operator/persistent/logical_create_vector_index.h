#pragma once

#include "binder/expression/expression.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/copier_config/hnsw_reader_config.h"
#include "planner/operator/logical_operator.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

class LogicalCreateVectorIndex : public LogicalOperator {
public:
    LogicalCreateVectorIndex(std::shared_ptr<Expression> internalId,
        std::shared_ptr<Expression> embedding, table_id_t tableID, property_id_t propertyID,
        common::HnswReaderConfig hnswReaderConfig, binder::expression_vector outExprs, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::CREATE_VECTOR_INDEX, std::move(child)},
          internalId{internalId}, embedding{embedding}, tableID{tableID}, propertyID{propertyID},
          hnswReaderConfig{hnswReaderConfig}, outExprs{std::move(outExprs)} {}

    inline std::string getExpressionsForPrinting() const override { return "CREATE_VECTOR_INDEX"; }

    inline std::shared_ptr<Expression> getInternalId() const { return internalId; }

    inline std::shared_ptr<Expression> getEmbedding() const { return embedding; }

    inline table_id_t getTableID() const { return tableID; }

    inline property_id_t getPropertyID() const { return propertyID; }

    inline common::HnswReaderConfig getHnswReaderConfig() const { return hnswReaderConfig; }

    inline binder::expression_vector getOutExprs() const { return outExprs; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalCreateVectorIndex>(internalId, embedding, tableID,
            propertyID, hnswReaderConfig, outExprs, children[0]->copy());
    }

private:
    std::shared_ptr<Expression> internalId;
    std::shared_ptr<Expression> embedding;
    table_id_t tableID;
    property_id_t propertyID;
    common::HnswReaderConfig hnswReaderConfig;
    binder::expression_vector outExprs;
};

} // namespace planner
} // namespace kuzu
