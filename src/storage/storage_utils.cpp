#include "storage/storage_utils.h"

#include "common/assert.h"
#include "common/null_buffer.h"
#include "common/string_format.h"
#include "common/types/ku_list.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::string StorageUtils::getColumnName(const std::string& propertyName, ColumnType type,
    const std::string& prefix) {
    switch (type) {
    case ColumnType::DATA: {
        return stringFormat("{}_data", propertyName);
    }
    case ColumnType::NULL_MASK: {
        return stringFormat("{}_null", propertyName);
    }
    case ColumnType::INDEX: {
        return stringFormat("{}_index", propertyName);
    }
    case ColumnType::OFFSET: {
        return stringFormat("{}_offset", propertyName);
    }
    case ColumnType::CSR_OFFSET: {
        return stringFormat("{}_csr_offset", prefix);
    }
    case ColumnType::CSR_LENGTH: {
        return stringFormat("{}_csr_length", prefix);
    }
    case ColumnType::STRUCT_CHILD: {
        return stringFormat("{}_{}_child", propertyName, prefix);
    }
    default: {
        if (prefix.empty()) {
            return propertyName;
        }
        return stringFormat("{}_{}", prefix, propertyName);
    }
    }
}

uint32_t StorageUtils::getDataTypeSize(PhysicalTypeID type) {
    switch (type) {
    case PhysicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return sizeof(ku_list_t);
    }
    case PhysicalTypeID::STRUCT: {
        // Not calculable using this interface!
        KU_UNREACHABLE;
    }
    default: {
        return PhysicalTypeUtils::getFixedTypeSize(type);
    }
    }
}

uint32_t StorageUtils::getDataTypeSize(const LogicalType& type) {
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return sizeof(ku_list_t);
    }
    case PhysicalTypeID::STRUCT: {
        uint32_t size = 0;
        const auto fieldsTypes = StructType::getFieldTypes(type);
        for (const auto& fieldType : fieldsTypes) {
            size += getDataTypeSize(*fieldType);
        }
        size += NullBuffer::getNumBytesForNullValues(fieldsTypes.size());
        return size;
    }
    default: {
        return PhysicalTypeUtils::getFixedTypeSize(type.getPhysicalType());
    }
    }
}

} // namespace storage
} // namespace kuzu
