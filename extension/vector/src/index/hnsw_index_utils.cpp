#include "index/hnsw_index_utils.h"

#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/types/types.h"
#include "simsimd.h"
#include "transaction/transaction_context.h"

namespace kuzu {
namespace vector_extension {

bool HNSWIndexUtils::indexExists(const main::ClientContext& context,
    const transaction::Transaction* transaction, const catalog::TableCatalogEntry* tableEntry,
    const std::string& indexName) {
    return catalog::Catalog::Get(context)->containsIndex(transaction, tableEntry->getTableID(),
        indexName);
}

bool HNSWIndexUtils::validateIndexExistence(const main::ClientContext& context,
    const catalog::TableCatalogEntry* tableEntry, const std::string& indexName,
    IndexOperation indexOperation, common::ConflictAction conflictAction) {
    auto transaction = transaction::Transaction::Get(context);
    switch (indexOperation) {
    case IndexOperation::CREATE: {
        if (indexExists(context, transaction, tableEntry, indexName)) {
            switch (conflictAction) {
            case common::ConflictAction::ON_CONFLICT_THROW:
                throw common::BinderException{common::stringFormat(
                    "Index {} already exists in table {}.", indexName, tableEntry->getName())};
            case common::ConflictAction::ON_CONFLICT_DO_NOTHING:
                return true;
            default:
                KU_UNREACHABLE;
            }
        }
        return false;
    } break;
    case IndexOperation::DROP: {
        if (!indexExists(context, transaction, tableEntry, indexName)) {
            switch (conflictAction) {
            case common::ConflictAction::ON_CONFLICT_THROW:
                throw common::BinderException{
                    common::stringFormat("Table {} doesn't have an index with name {}.",
                        tableEntry->getName(), indexName)};
            case common::ConflictAction::ON_CONFLICT_DO_NOTHING:
                return false;
            default:
                KU_UNREACHABLE;
            }
        }
        return true;
    } break;
    case IndexOperation::QUERY: {
        if (!indexExists(context, transaction, tableEntry, indexName)) {
            throw common::BinderException{common::stringFormat(
                "Table {} doesn't have an index with name {}.", tableEntry->getName(), indexName)};
        }
        return true;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

catalog::NodeTableCatalogEntry* HNSWIndexUtils::bindNodeTable(const main::ClientContext& context,
    const std::string& tableName) {
    binder::Binder::validateTableExistence(context, tableName);
    auto transaction = transaction::Transaction::Get(context);
    const auto tableEntry =
        catalog::Catalog::Get(context)->getTableCatalogEntry(transaction, tableName);
    binder::Binder::validateNodeTableType(tableEntry);
    return tableEntry->ptrCast<catalog::NodeTableCatalogEntry>();
}

void HNSWIndexUtils::validateAutoTransaction(const main::ClientContext& context,
    const std::string& funcName) {
    if (!transaction::TransactionContext::Get(context)->isAutoTransaction()) {
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

template<auto Func, VectorElementType T>
static double computeDistance(const void* left, const void* right, uint32_t dimension) {
    double distance = 0.0;
    Func(static_cast<const T*>(left), static_cast<const T*>(right), dimension, &distance);
    return distance;
}

template<auto FUNC_F32, auto FUNC_F64>
static metric_func_t computeDistanceFuncDispatch(const common::LogicalType& type) {
    switch (type.getLogicalTypeID()) {
    case common::LogicalTypeID::FLOAT: {
        return computeDistance<FUNC_F32, float>;
    }
    case common::LogicalTypeID::DOUBLE: {
        return computeDistance<FUNC_F64, double>;
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

metric_func_t HNSWIndexUtils::getMetricsFunction(MetricType metric,
    const common::LogicalType& type) {
    switch (metric) {
    case MetricType::Cosine: {
        return computeDistanceFuncDispatch<simsimd_cos_f32, simsimd_cos_f64>(type);
    }
    case MetricType::DotProduct: {
        return computeDistanceFuncDispatch<simsimd_dot_f32, simsimd_dot_f64>(type);
    }
    case MetricType::L2: {
        return computeDistanceFuncDispatch<simsimd_l2_f32, simsimd_l2_f64>(type);
    }
    case MetricType::L2_SQUARE: {
        return computeDistanceFuncDispatch<simsimd_l2sq_f32, simsimd_l2sq_f64>(type);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

void HNSWIndexUtils::validateColumnType(const common::LogicalType& type) {
    if (type.getLogicalTypeID() == common::LogicalTypeID::ARRAY) {
        auto& childType =
            type.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>()->getChildType();
        if (childType.getLogicalTypeID() == common::LogicalTypeID::FLOAT ||
            childType.getLogicalTypeID() == common::LogicalTypeID::DOUBLE) {
            return;
        }
    }
    throw common::BinderException("VECTOR_INDEX only supports FLOAT/DOUBLE ARRAY columns.");
}

} // namespace vector_extension
} // namespace kuzu
