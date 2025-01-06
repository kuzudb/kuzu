#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "planner/operator/ddl/logical_create_vector_index.h"
#include "planner/operator/logical_update_vector_index.h"
#include "processor/operator/ddl/create_vector_index.h"
#include "processor/operator/persistent/batch_insert.h"
#include "processor/operator/persistent/rel_batch_insert.h"
#include "processor/operator/persistent/vector_index/bulk_vector_indexing.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"
#include "storage/storage_manager.h"
#include "main/db_config.h"

using namespace kuzu::planner;
using namespace kuzu::catalog;

namespace kuzu {
    namespace processor {
        std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateVectorIndex(
                planner::LogicalOperator *logicalOperator) {
            auto &logical = logicalOperator->constCast<LogicalCreateVectorIndex>();
            auto printInfo = std::make_unique<OPPrintInfo>(logical.getExpressionsForPrinting());
            auto outSchema = logical.getSchema();
            auto outputDataPos = DataPos(outSchema->getExpressionPos(*logical.getOutputExpression()));

            // TODO: move it to binder
            // Get table entry
            auto tableEntry = ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(
                    clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(),
                                                                      logical.getTableID()));
            auto property = tableEntry->getProperty(logical.getPropertyID());
            auto dim = ArrayType::getNumElements(property->getDataType());

            return std::make_unique<CreateVectorIndex>(logical.getTableName(), logical.getPropertyName(),
                                                       logical.getTableID(), logical.getPropertyID(), dim,
                                                       logical.getVectorIndexConfig(),
                                                       outputDataPos, getOperatorID(), std::move(printInfo));
        }

        std::unique_ptr<PhysicalOperator> PlanMapper::mapUpdateVectorIndex(
                LogicalOperator *logicalOperator) {
            auto updateVectorIndex = logicalOperator->ptrCast<LogicalUpdateVectorIndex>();
            auto storageManager = clientContext->getStorageManager();
            auto header = storageManager->getVectorIndexHeaderWriteVersion(updateVectorIndex->getTableId(),
                                                                           updateVectorIndex->getPropertyId());
            KU_ASSERT_MSG(header != nullptr, "Vector index header not found");
            auto nodeTable =
                    ku_dynamic_cast<Table *, NodeTable *>(storageManager->getTable(header->getNodeTableId()));
            auto maxNodeTableOffset = nodeTable->getMaxNodeOffset(clientContext->getTx());
            auto fTable =
                    FactorizedTableUtils::getSingleStringColumnFTable(clientContext->getMemoryManager());
            auto fTableScan = createFTableScanAligned(updateVectorIndex->getOutExprs(), updateVectorIndex->getSchema(),
                                                      fTable,
                                                      DEFAULT_VECTOR_CAPACITY /* maxMorselSize */);
            // First map all other child operators
            for (uint64_t i = 0; i < logicalOperator->getNumChildren(); i++) {
                if (i >= 1) {
                    updateVectorIndex = logicalOperator->getChild(i)->ptrCast<LogicalUpdateVectorIndex>();
                }
                auto partitionId = updateVectorIndex->getPartitionId();
                auto partitionHeader = header->getPartitionHeader(partitionId);
                KU_ASSERT_MSG(partitionHeader != nullptr, "Partition header not found");
                auto prevOperator = mapOperator(updateVectorIndex->getChild(0).get());
                auto printInfo = std::make_unique<OPPrintInfo>(updateVectorIndex->getExpressionsForPrinting());
                auto outSchema = updateVectorIndex->getSchema();
                auto offsetDataPos = DataPos(outSchema->getExpressionPos(*updateVectorIndex->getOffset()));
                auto embeddingDataPos = DataPos(
                        outSchema->getExpressionPos(*updateVectorIndex->getEmbeddingProperty()));
                auto localState =
                        std::make_unique<BulkVectorIndexingLocalState>(offsetDataPos, embeddingDataPos);
                auto relTable =
                        ku_dynamic_cast<Table *, RelTable *>(
                                storageManager->getTable(partitionHeader->getCSRRelTableId()));
                auto relTableEntry = clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(),
                                                                                       relTable->getTableID());
                auto maxOffset = partitionHeader->getMaxOffset(maxNodeTableOffset);
                auto numPartitions = ceil((double) maxOffset / StorageConstants::NODE_GROUP_SIZE);
                // init partitioner shared state
                auto partitionerSharedState = std::make_shared<PartitionerSharedState>();
                partitionerSharedState->srcNodeTable = nodeTable;
                partitionerSharedState->dstNodeTable = nodeTable;
                partitionerSharedState->maxNodeOffsets.push_back(maxOffset);
                partitionerSharedState->numPartitions.push_back(numPartitions);
                partitionerSharedState->partitioningBuffers.push_back(std::make_unique<PartitioningBuffer>());
                auto sharedState = std::make_shared<BulkVectorIndexingSharedState>(
                        header, partitionId,
                        partitionerSharedState,
                        std::min(clientContext->getDBConfig()->maxNumThreads, clientContext->getMaxNumThreadForExec()));
                auto bulkIndexing = std::make_unique<BulkVectorIndexing>(
                        std::make_unique<ResultSetDescriptor>(outSchema), std::move(localState), sharedState,
                        std::move(prevOperator), getOperatorID(), std::move(printInfo));
                auto batchInsertSharedState =
                        std::make_shared<BatchInsertSharedState>(relTable, fTable, &storageManager->getWAL());
                std::vector<common::LogicalType> columnTypes;
                columnTypes.push_back(LogicalType::INTERNAL_ID()); // NBR
                for (auto &property: relTableEntry->getPropertiesRef()) {
                    columnTypes.push_back(property.getDataType().copy());
                }
                auto relBatchInsertInfo = std::make_unique<RelBatchInsertInfo>(relTableEntry,
                                                                               clientContext->getStorageManager()->compressionEnabled(),
                                                                               RelDataDirection::FWD, 0, 0,
                                                                               std::move(columnTypes));
                auto relBatchInsertPrintInfo =
                        std::make_unique<OPPrintInfo>(relTableEntry->getName());
                auto relBatchInsert = std::make_unique<RelBatchInsert>(std::move(relBatchInsertInfo),
                                                                       std::move(partitionerSharedState),
                                                                       std::move(batchInsertSharedState),
                                                                       std::make_unique<ResultSetDescriptor>(outSchema),
                                                                       getOperatorID(),
                                                                       std::move(relBatchInsertPrintInfo),
                                                                       RelDataDirection::FWD);
                fTableScan->addChild(std::move(relBatchInsert));
                fTableScan->addChild(std::move(bulkIndexing));
            }
            return fTableScan;
        }
    } // namespace processor
} // namespace kuzu
