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
    auto type = TableType::UNKNOWN;
    std::string tableName;
    auto onConflict = ConflictAction::INVALID;
    bool hasParent = false;
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

std::string BoundCreateTableInfo::toString() const {
    std::string result = "";
    switch (type) {
    case TableType::NODE: {
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
    case TableType::REL: {
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
    case TableType::REL_GROUP: {
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
    RelMultiplicity srcMultiplicity{};
    RelMultiplicity dstMultiplicity{};
    table_id_t srcTableID = INVALID_TABLE_ID;
    table_id_t dstTableID = INVALID_TABLE_ID;
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

} // namespace binder
} // namespace kuzu
