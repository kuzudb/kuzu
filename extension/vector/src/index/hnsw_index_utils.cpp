#include "index/hnsw_index_utils.h"

#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "simsimd.h"

namespace kuzu {
namespace vector_extension {

void HNSWIndexUtils::validateIndexExistence(const main::ClientContext& context,
    const catalog::TableCatalogEntry* tableEntry, const std::string& indexName,
    IndexOperation indexOperation) {
    switch (indexOperation) {
    case IndexOperation::CREATE: {
        if (context.getCatalog()->containsIndex(context.getTransaction(), tableEntry->getTableID(),
                indexName)) {
            throw common::BinderException{common::stringFormat(
                "Index {} already exists in table {}.", indexName, tableEntry->getName())};
        }
    } break;
    case IndexOperation::DROP:
    case IndexOperation::QUERY: {
        if (!context.getCatalog()->containsIndex(context.getTransaction(), tableEntry->getTableID(),
                indexName)) {
            throw common::BinderException{common::stringFormat(
                "Table {} doesn't have an index with name {}.", tableEntry->getName(), indexName)};
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

catalog::NodeTableCatalogEntry* HNSWIndexUtils::bindNodeTable(const main::ClientContext& context,
    const std::string& tableName, const std::string& indexName, IndexOperation indexOperation) {
    binder::Binder::validateTableExistence(context, tableName);
    const auto tableEntry =
        context.getCatalog()->getTableCatalogEntry(context.getTransaction(), tableName);
    binder::Binder::validateNodeTableType(tableEntry);
    validateIndexExistence(context, tableEntry, indexName, indexOperation);
    return tableEntry->ptrCast<catalog::NodeTableCatalogEntry>();
}

void HNSWIndexUtils::validateAutoTransaction(const main::ClientContext& context,
    const std::string& funcName) {
    if (!context.getTransactionContext()->isAutoTransaction()) {
        throw common::BinderException{
            common::stringFormat("{} is only supported in auto transaction mode.", funcName)};
    }
}

void HNSWIndexUtils::validateColumnType(const catalog::TableCatalogEntry& tableEntry,
    const std::string& columnName) {
    binder::Binder::validateColumnExistence(&tableEntry, columnName);
    auto& type = tableEntry.getProperty(columnName).getType();
    validateColumnType(type);
}

double HNSWIndexUtils::computeDistance(DistFuncType funcType, const float* left, const float* right,
    uint32_t dimension) {
    double distance = 0.0;
    switch (funcType) {
    case DistFuncType::Cosine: {
        simsimd_cos_f32(left, right, dimension, &distance);
    } break;
    case DistFuncType::DotProduct: {
        simsimd_dot_f32(left, right, dimension, &distance);
    } break;
    case DistFuncType::L2: {
        // L2 distance is the square root of the sum of the squared differences between the two
        // vectors. Also known as the Euclidean distance.
        simsimd_l2_f32(left, right, dimension, &distance);
    } break;
    case DistFuncType::L2_SQUARE: {
        // L2 square distance is the sum of the squared differences between the two vectors.
        // Also known as the squared Euclidean distance.
        simsimd_l2sq_f32(left, right, dimension, &distance);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return distance;
}

void HNSWIndexUtils::validateColumnType(const common::LogicalType& type) {
    if (type.getLogicalTypeID() != common::LogicalTypeID::ARRAY ||
        type.getExtraTypeInfo()
                ->constPtrCast<common::ArrayTypeInfo>()
                ->getChildType()
                .getLogicalTypeID() != common::LogicalTypeID::FLOAT) {
        throw common::BinderException("HNSW_INDEX only supports FLOAT ARRAY columns.");
    }
}

} // namespace vector_extension
} // namespace kuzu
