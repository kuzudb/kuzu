#include "catalog/rel_table_schema.h"

#include "common/exception.h"
#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

RelMultiplicity getRelMultiplicityFromString(const std::string& relMultiplicityString) {
    if ("ONE_ONE" == relMultiplicityString) {
        return RelMultiplicity::ONE_ONE;
    } else if ("MANY_ONE" == relMultiplicityString) {
        return RelMultiplicity::MANY_ONE;
    } else if ("ONE_MANY" == relMultiplicityString) {
        return RelMultiplicity::ONE_MANY;
    } else if ("MANY_MANY" == relMultiplicityString) {
        return RelMultiplicity::MANY_MANY;
    }
    throw CatalogException("Invalid relMultiplicity string '" + relMultiplicityString + "'.");
}

std::string getRelMultiplicityAsString(RelMultiplicity relMultiplicity) {
    switch (relMultiplicity) {
    case RelMultiplicity::MANY_MANY: {
        return "MANY_MANY";
    }
    case RelMultiplicity::MANY_ONE: {
        return "MANY_ONE";
    }
    case RelMultiplicity::ONE_ONE: {
        return "ONE_ONE";
    }
    case RelMultiplicity::ONE_MANY: {
        return "ONE_MANY";
    }
    default:
        throw CatalogException("Cannot convert rel multiplicity to std::string.");
    }
}

void RelTableSchema::serializeInternal(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(relMultiplicity, fileInfo, offset);
    SerDeser::serializeValue(srcTableID, fileInfo, offset);
    SerDeser::serializeValue(dstTableID, fileInfo, offset);
    srcPKDataType->serialize(fileInfo, offset);
    dstPKDataType->serialize(fileInfo, offset);
}

std::unique_ptr<RelTableSchema> RelTableSchema::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    RelMultiplicity relMultiplicity;
    table_id_t srcTableID;
    table_id_t dstTableID;
    SerDeser::deserializeValue(relMultiplicity, fileInfo, offset);
    SerDeser::deserializeValue(srcTableID, fileInfo, offset);
    SerDeser::deserializeValue(dstTableID, fileInfo, offset);
    auto srcPKDataType = LogicalType::deserialize(fileInfo, offset);
    auto dstPKDataType = LogicalType::deserialize(fileInfo, offset);
    return std::make_unique<RelTableSchema>(relMultiplicity, srcTableID, dstTableID,
        std::move(srcPKDataType), std::move(dstPKDataType));
}

} // namespace catalog
} // namespace kuzu
