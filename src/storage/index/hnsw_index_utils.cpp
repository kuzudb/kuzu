#include "storage/index/hnsw_index_utils.h"

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/enums/table_type.h"
#include "common/exception/binder.h"
#include "common/types/types.h"
#include "function/array/functions/array_distance.h"
#include "storage/index/hnsw_graph.h"

namespace kuzu {
namespace storage {

void HNSWIndexUtils::validateTableAndColumnType(const catalog::TableCatalogEntry& tableEntry,
    const std::string& columnName) {
    if (tableEntry.getTableType() != common::TableType::NODE) {
        throw common::BinderException("HNSW_INDEX only supports NODE tables.");
    }
    auto& type = tableEntry.getProperty(columnName).getType();
    validateColumnType(type);
}

void HNSWIndexUtils::validateQueryVector(const common::LogicalType& type, uint64_t dimension) {
    validateColumnType(type);
    if (type.getExtraTypeInfo()->constPtrCast<common::ArrayTypeInfo>()->getNumElements() !=
        dimension) {
        throw common::BinderException("Query vector dimension does not match index dimension.");
    }
}

float HNSWIndexUtils::computeDistance(const EmbeddingColumn& embeddings, const float* left,
    common::offset_t right, transaction::Transaction* transaction) {
    const auto dimension = embeddings.getTypeInfo().dimension;
    float distance = 0.0;
    KU_ASSERT(
        embeddings.getTypeInfo().elementType.getPhysicalType() == common::PhysicalTypeID::FLOAT);
    function::ArrayDistance::operation<float>(left, embeddings.getEmebdding(right, transaction),
        dimension, distance);
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

} // namespace storage
} // namespace kuzu
