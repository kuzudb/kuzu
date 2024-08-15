#include "binder/ddl/bound_create_table_info.h"

#include "binder/binder.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::parser;
using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

void BoundCreateTableInfo::serialize(Serializer& serializer) const {
    serializer.serializeValue(type);
    serializer.serializeValue(tableName);
    serializer.serializeValue(onConflict);
    serializer.serializeValue(hasParent);
    extraInfo->serialize(serializer);
}

BoundCreateTableInfo BoundCreateTableInfo::deserialize(Deserializer& deserializer) {
    TableType type;
    std::string tableName;
    ConflictAction onConflict;
    bool hasParent;
    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> extraInfo;
    deserializer.deserializeValue(type);
    deserializer.deserializeValue(tableName);
    deserializer.deserializeValue(onConflict);
    deserializer.deserializeValue(hasParent);
    switch (type) {
    case TableType::NODE:
    case TableType::REL: {
        extraInfo = BoundExtraCreateTableInfo::deserialize(deserializer, type);
    } break;
    case TableType::RDF: {
        extraInfo = BoundExtraCreateRdfGraphInfo::deserialize(deserializer);
    } break;
    case TableType::REL_GROUP: {
        extraInfo = BoundExtraCreateRelTableGroupInfo::deserialize(deserializer);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    auto retval = BoundCreateTableInfo{type, tableName, onConflict, std::move(extraInfo)};
    retval.hasParent = hasParent;
    return retval;
}

void BoundExtraCreateTableInfo::serialize(Serializer& serializer) const {
    serializer.serializeVector(propertyDefinitions);
}

std::unique_ptr<BoundExtraCreateTableInfo> BoundExtraCreateTableInfo::deserialize(
    Deserializer& deserializer, TableType type) {
    std::vector<PropertyDefinition> propertyDefinitions;
    std::unique_ptr<BoundExtraCreateTableInfo> info;
    deserializer.deserializeVector(propertyDefinitions);
    switch (type) {
    case TableType::NODE: {
        info = BoundExtraCreateNodeTableInfo::deserialize(deserializer);
    } break;
    case TableType::REL: {
        info = BoundExtraCreateRelTableInfo::deserialize(deserializer);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    info->propertyDefinitions = std::move(propertyDefinitions);
    return info;
}

void BoundExtraCreateNodeTableInfo::serialize(Serializer& serializer) const {
    BoundExtraCreateTableInfo::serialize(serializer);
    serializer.serializeValue(primaryKeyName);
}

std::unique_ptr<BoundExtraCreateNodeTableInfo> BoundExtraCreateNodeTableInfo::deserialize(
    Deserializer& deserializer) {
    std::string primaryKeyName;
    deserializer.deserializeValue(primaryKeyName);
    return std::make_unique<BoundExtraCreateNodeTableInfo>(primaryKeyName,
        std::vector<PropertyDefinition>());
}

void BoundExtraCreateRelTableInfo::serialize(Serializer& serializer) const {
    BoundExtraCreateTableInfo::serialize(serializer);
    serializer.serializeValue(srcMultiplicity);
    serializer.serializeValue(dstMultiplicity);
    serializer.serializeValue(srcTableID);
    serializer.serializeValue(dstTableID);
}

std::unique_ptr<BoundExtraCreateRelTableInfo> BoundExtraCreateRelTableInfo::deserialize(
    Deserializer& deserializer) {
    common::RelMultiplicity srcMultiplicity;
    common::RelMultiplicity dstMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    deserializer.deserializeValue(srcMultiplicity);
    deserializer.deserializeValue(dstMultiplicity);
    deserializer.deserializeValue(srcTableID);
    deserializer.deserializeValue(dstTableID);
    return std::make_unique<BoundExtraCreateRelTableInfo>(srcMultiplicity, dstMultiplicity,
        srcTableID, dstTableID, std::vector<PropertyDefinition>());
}

void BoundExtraCreateRelTableGroupInfo::serialize(Serializer& serializer) const {
    serializer.serializeVector(infos);
}

std::unique_ptr<BoundExtraCreateRelTableGroupInfo> BoundExtraCreateRelTableGroupInfo::deserialize(
    Deserializer& deserializer) {
    std::vector<BoundCreateTableInfo> infos;
    deserializer.deserializeVector(infos);
    return std::make_unique<BoundExtraCreateRelTableGroupInfo>(std::move(infos));
}

void BoundExtraCreateRdfGraphInfo::serialize(Serializer& serializer) const {
    resourceInfo.serialize(serializer);
    literalInfo.serialize(serializer);
    resourceTripleInfo.serialize(serializer);
    literalTripleInfo.serialize(serializer);
}

std::unique_ptr<BoundExtraCreateRdfGraphInfo> BoundExtraCreateRdfGraphInfo::deserialize(
    Deserializer& deserializer) {
    auto resourceInfo = BoundCreateTableInfo::deserialize(deserializer);
    auto literalInfo = BoundCreateTableInfo::deserialize(deserializer);
    auto resourceTripleInfo = BoundCreateTableInfo::deserialize(deserializer);
    auto literalTripleInfo = BoundCreateTableInfo::deserialize(deserializer);
    return std::make_unique<BoundExtraCreateRdfGraphInfo>(std::move(resourceInfo),
        std::move(literalInfo), std::move(resourceTripleInfo), std::move(literalTripleInfo));
}

} // namespace binder
} // namespace kuzu
