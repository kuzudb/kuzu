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
                                     int partitionId,
                                     const logical_op_vector_t& children)
                    : LogicalOperator{LogicalOperatorType::UPDATE_VECTOR_INDEX, std::move(children)},
                      tableName{std::move(tableName)}, propertyName{std::move(propertyName)}, tableId{tableId},
                      propertyId{propertyId}, offset{std::move(offset)},
                      embeddingProperty{std::move(embeddingProperty)}, outExprs{std::move(outExprs)},
                      partitionId(partitionId) {}

            std::string getExpressionsForPrinting() const override {
                return stringFormat("BulkIndexing_{}_{}_{}", tableName, propertyName, partitionId);
            }

            void computeFactorizedSchema() override;

            void computeFlatSchema() override;

            inline table_id_t getTableId() const { return tableId; }

            inline property_id_t getPropertyId() const { return propertyId; }

            inline std::shared_ptr<Expression> getOffset() const { return offset; }

            inline std::shared_ptr<Expression> getEmbeddingProperty() const { return embeddingProperty; }

            inline const expression_vector &getOutExprs() const { return outExprs; }

            inline int getPartitionId() const { return partitionId; }

            inline bool isMainPartition() const { return partitionId == 0; }

            std::unique_ptr<LogicalOperator> copy() override {
                return make_unique<LogicalUpdateVectorIndex>(tableName, propertyName, tableId, propertyId,
                                                             offset, embeddingProperty, outExprs, partitionId,
                                                             copyVector(children));
            }

        private:
            std::string tableName;
            std::string propertyName;
            table_id_t tableId;
            property_id_t propertyId;
            std::shared_ptr<binder::Expression> offset;
            std::shared_ptr<binder::Expression> embeddingProperty;
            binder::expression_vector outExprs;
            int partitionId;
        };

    } // namespace planner
} // namespace kuzu
