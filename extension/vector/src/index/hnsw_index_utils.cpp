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
