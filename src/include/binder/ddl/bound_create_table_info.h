#pragma once

#include "catalog/table_schema.h"

namespace kuzu {
namespace binder {

struct BoundExtraCreateTableInfo {
    virtual ~BoundExtraCreateTableInfo() = default;
    virtual inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const = 0;
};

struct BoundCreateTableInfo {
    std::string tableName;
    std::vector<std::unique_ptr<catalog::Property>> properties;
    std::unique_ptr<BoundExtraCreateTableInfo> extraInfo;

    BoundCreateTableInfo(
        std::string tableName, std::unique_ptr<BoundExtraCreateTableInfo> extraInfo)
        : tableName{std::move(tableName)}, extraInfo{std::move(extraInfo)} {}
    BoundCreateTableInfo(std::string tableName,
        std::vector<std::unique_ptr<catalog::Property>> properties,
        std::unique_ptr<BoundExtraCreateTableInfo> extraInfo)
        : tableName{std::move(tableName)}, properties{std::move(properties)}, extraInfo{std::move(
                                                                                  extraInfo)} {}
    BoundCreateTableInfo(const BoundCreateTableInfo& other)
        : tableName{other.tableName}, extraInfo{other.extraInfo->copy()} {
        for (auto& property : other.properties) {
            properties.push_back(property->copy());
        }
    }

    inline std::unique_ptr<BoundCreateTableInfo> copy() const {
        return std::make_unique<BoundCreateTableInfo>(*this);
    }
};

struct BoundNodeExtraCreateTableInfo : public BoundExtraCreateTableInfo {
    common::property_id_t primaryKeyIdx;

    explicit BoundNodeExtraCreateTableInfo(common::property_id_t primaryKeyIdx)
        : primaryKeyIdx{primaryKeyIdx} {}
    BoundNodeExtraCreateTableInfo(const BoundNodeExtraCreateTableInfo& other)
        : primaryKeyIdx{other.primaryKeyIdx} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundNodeExtraCreateTableInfo>(*this);
    }
};

struct BoundRelExtraCreateTableInfo : public BoundExtraCreateTableInfo {
    catalog::RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    std::unique_ptr<common::LogicalType> srcPkDataType;
    std::unique_ptr<common::LogicalType> dstPkDataType;

    BoundRelExtraCreateTableInfo(catalog::RelMultiplicity relMultiplicity,
        common::table_id_t srcTableID, common::table_id_t dstTableID,
        std::unique_ptr<common::LogicalType> srcPkDataType,
        std::unique_ptr<common::LogicalType> dstPkDataType)
        : relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          srcPkDataType{std::move(srcPkDataType)}, dstPkDataType{std::move(dstPkDataType)} {}
    BoundRelExtraCreateTableInfo(const BoundRelExtraCreateTableInfo& other)
        : relMultiplicity{other.relMultiplicity}, srcTableID{other.srcTableID},
          dstTableID{other.dstTableID}, srcPkDataType{other.srcPkDataType->copy()},
          dstPkDataType{other.dstPkDataType->copy()} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundRelExtraCreateTableInfo>(*this);
    }
};

struct BoundRdfExtraCreateTableInfo : public BoundExtraCreateTableInfo {
    std::unique_ptr<BoundCreateTableInfo> nodeInfo;
    std::unique_ptr<BoundCreateTableInfo> relInfo;

    BoundRdfExtraCreateTableInfo(std::unique_ptr<BoundCreateTableInfo> nodeInfo,
        std::unique_ptr<BoundCreateTableInfo> relInfo)
        : nodeInfo{std::move(nodeInfo)}, relInfo{std::move(relInfo)} {}
    BoundRdfExtraCreateTableInfo(const BoundRdfExtraCreateTableInfo& other)
        : nodeInfo{other.nodeInfo->copy()}, relInfo{other.relInfo->copy()} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundRdfExtraCreateTableInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
