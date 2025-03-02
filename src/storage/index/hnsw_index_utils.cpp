#include "storage/index/hnsw_index_utils.h"

#include "binder/binder.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/types/types.h"
#include "simsimd.h"

namespace kuzu {
namespace storage {

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

} // namespace storage
} // namespace kuzu
