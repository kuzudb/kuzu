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
    serializer.serializeValue(isInternal);
    extraInfo->serialize(serializer);
}

BoundCreateTableInfo BoundCreateTableInfo::deserialize(Deserializer& deserializer) {
    auto type = CatalogEntryType::DUMMY_ENTRY;
    std::string tableName;
    auto onConflict = ConflictAction::INVALID;
    bool hasParent = false;
    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> extraInfo;
    bool isInternal = false;
    deserializer.deserializeValue(type);
    deserializer.deserializeValue(tableName);
    deserializer.deserializeValue(onConflict);
    deserializer.deserializeValue(hasParent);
    deserializer.deserializeValue(isInternal);
    switch (type) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_TABLE_ENTRY: {
        extraInfo = BoundExtraCreateTableInfo::deserialize(deserializer, type);
    } break;
    case CatalogEntryType::REL_GROUP_ENTRY: {
        extraInfo = BoundExtraCreateRelTableGroupInfo::deserialize(deserializer);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    auto retval =
        BoundCreateTableInfo{type, tableName, onConflict, std::move(extraInfo), isInternal};
    retval.hasParent = hasParent;
    return retval;
}

std::string BoundCreateTableInfo::toString() const {
    std::string result = "";
    switch (type) {
    case CatalogEntryType::NODE_TABLE_ENTRY: {
        result += "Create Node Table: ";
        result += tableName;
        result += ",Properties: ";
        auto nodeInfo = extraInfo->ptrCast<BoundExtraCreateNodeTableInfo>();
        for (auto& definition : nodeInfo->propertyDefinitions) {
            result += definition.getName();
            result += ", ";
        }
        break;
    }
    case CatalogEntryType::REL_TABLE_ENTRY: {
        result += "Create Relationship Table: ";
        result += tableName;
        result += ",Multiplicity: ";
        auto* relInfo = extraInfo->ptrCast<BoundExtraCreateRelTableInfo>();
        if (relInfo->srcMultiplicity == RelMultiplicity::ONE) {
            result += "ONE";
        } else {
            result += "MANY";
        }
        result += "_";
        if (relInfo->dstMultiplicity == RelMultiplicity::ONE) {
            result += "ONE";
        } else {
            result += "MANY";
        }
        result += ",Properties: ";
        for (auto& definition : relInfo->propertyDefinitions) {
            result += definition.getName();
            result += ", ";
        }
        break;
    }
    case CatalogEntryType::REL_GROUP_ENTRY: {
        result += "Create Relationship Group Table: ";
        result += tableName;
        auto* relGroupInfo = extraInfo->ptrCast<BoundExtraCreateRelTableGroupInfo>();
        result += ",Tables: ";
        for (auto& createInfo : relGroupInfo->infos) {
            result += createInfo.tableName;
            result += ", ";
        }
        auto* groupTableInfo =
            relGroupInfo->infos[0].extraInfo->ptrCast<BoundExtraCreateTableInfo>();
        result += "Properties: ";
        for (auto& definition : groupTableInfo->propertyDefinitions) {
            result += definition.getName();
            result += ", ";
        }
        break;
    }
    default:
        break;
    }
    return result;
}

void BoundExtraCreateTableInfo::serialize(Serializer& serializer) const {
    serializer.serializeVector(propertyDefinitions);
}

std::unique_ptr<BoundExtraCreateTableInfo> BoundExtraCreateTableInfo::deserialize(
    Deserializer& deserializer, CatalogEntryType type) {
    std::vector<PropertyDefinition> propertyDefinitions;
    std::unique_ptr<BoundExtraCreateTableInfo> info;
    deserializer.deserializeVector(propertyDefinitions);
    switch (type) {
    case CatalogEntryType::NODE_TABLE_ENTRY: {
        info = BoundExtraCreateNodeTableInfo::deserialize(deserializer);
    } break;
    case CatalogEntryType::REL_TABLE_ENTRY: {
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
    serializer.serializeValue(storageDirection);
    serializer.serializeValue(srcTableID);
    serializer.serializeValue(dstTableID);
}

std::unique_ptr<BoundExtraCreateRelTableInfo> BoundExtraCreateRelTableInfo::deserialize(
    Deserializer& deserializer) {
    RelMultiplicity srcMultiplicity{};
    RelMultiplicity dstMultiplicity{};
    ExtendDirection storageDirection{};
    table_id_t srcTableID = INVALID_TABLE_ID;
    table_id_t dstTableID = INVALID_TABLE_ID;
    deserializer.deserializeValue(srcMultiplicity);
    deserializer.deserializeValue(dstMultiplicity);
    deserializer.deserializeValue(storageDirection);
    deserializer.deserializeValue(srcTableID);
    deserializer.deserializeValue(dstTableID);
    return std::make_unique<BoundExtraCreateRelTableInfo>(srcMultiplicity, dstMultiplicity,
        storageDirection, srcTableID, dstTableID, std::vector<PropertyDefinition>());
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

} // namespace binder
} // namespace kuzu
