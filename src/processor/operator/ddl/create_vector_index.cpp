#include "processor/operator/ddl/create_vector_index.h"

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "common/string_format.h"
#include "storage/index/vector_index_header.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::storage;
using namespace kuzu::common;

namespace kuzu {
    namespace processor {
        void CreateVectorIndex::executeDDLInternal(ExecutionContext *context) {
            auto catalog = context->clientContext->getCatalog();
            auto storageManager = context->clientContext->getStorageManager();
            auto txn = context->clientContext->getTx();
            auto table = storageManager->getTable(tableId);

            // Add column to table for compressed vectors
            auto compressedPropertyName = VectorIndexHeader::getCompressedVectorPropertyName(propertyId);
            // Here we do not have any default value evaluator
            auto type = LogicalType::ARRAY(LogicalType::UINT8(), dim + 4);
            auto defaultValue =
                    std::make_unique<parser::ParsedLiteralExpression>(Value::createNullValue(type), "NULL");
            binder::BoundAlterInfo alterInfo(AlterType::ADD_PROPERTY, tableName, tableId,
                                             std::make_unique<binder::BoundExtraAddPropertyInfo>(
                                                     compressedPropertyName,
                                                     LogicalType::ARRAY(
                                                             LogicalType::UINT8(),
                                                             dim + 4),
                                                     std::move(defaultValue),
                                                     nullptr));
            catalog->alterTableSchema(context->clientContext->getTx(), alterInfo);
            auto schema = catalog->getTableCatalogEntry(context->clientContext->getTx(), tableId);
            auto addedPropId = schema->getPropertyID(compressedPropertyName);
            auto addedProp = schema->getProperty(addedPropId);
            table->addColumn(context->clientContext->getTx(), *addedProp, nullptr);
            auto header = std::make_unique<VectorIndexHeader>(dim, config, tableId, propertyId, addedPropId);

            // Create rel table per partition
            auto nodeTable = ku_dynamic_cast<Table *, NodeTable *>(table);
            auto numNodeGroups = nodeTable->getNumNodeGroups(txn);
            auto numNodeGroupsPerPartition = std::max(int(config.numberVectorsPerPartition / StorageConstants::NODE_GROUP_SIZE), 1);
            // 30% of the vectors per partition
            auto numVectorsThreshold = config.numberVectorsPerPartition * 0.3;

            // create rel table for each partition
            for (node_group_idx_t start = 0; start < numNodeGroups; start += numNodeGroupsPerPartition) {
                auto end = std::min(start + numNodeGroupsPerPartition - 1, numNodeGroups - 1);
                // Optimization where we avoid creating an index for last small partition, instead combine it with the
                // previous one. Maybe a better way is to create no index for the last partition and do parallel knn
                // search until there are enough vectors to create an index.
                auto leftNodeGroup = numNodeGroups - end - 1;
                auto breakLoop = false;
                if (leftNodeGroup * StorageConstants::NODE_GROUP_SIZE < numVectorsThreshold) {
                    end = numNodeGroups - 1;
                    breakLoop = true;
                }
                auto propertyInfos = std::vector<binder::PropertyInfo>();
                propertyInfos.emplace_back(InternalKeyword::ID, LogicalType::INTERNAL_ID());
                binder::BoundCreateTableInfo createTableInfo(common::TableType::REL,
                                                             VectorIndexHeader::getIndexRelTableName(tableId,
                                                                                                     propertyId, start,
                                                                                                     end),
                                                             ConflictAction::ON_CONFLICT_THROW,
                                                             std::make_unique<binder::BoundExtraCreateRelTableInfo>(
                                                                     RelMultiplicity::MANY,
                                                                     RelMultiplicity::MANY, tableId, tableId,
                                                                     std::move(propertyInfos)));
                auto relTableId = catalog->createTableSchema(context->clientContext->getTx(), createTableInfo);
                storageManager->createTable(relTableId, catalog, context->clientContext);
                header->createPartition(start, end, relTableId);
                if (breakLoop) {
                    break;
                }
            }
            storageManager->addVectorIndex(std::move(header));
        }

        std::string CreateVectorIndex::getOutputMsg() {
            return stringFormat("Vector index created on {}.{}", tableName, propertyName);
        }
    } // namespace processor
} // namespace kuzu
