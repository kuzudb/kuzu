#pragma once

#include "catalog/property.h"
#include "common/enums/table_type.h"

namespace kuzu {
namespace catalog {
enum class RelMultiplicity : uint8_t;
}
namespace binder {

struct BoundExtraCreateTableInfo {
    virtual ~BoundExtraCreateTableInfo() = default;
    virtual inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const = 0;
};

struct BoundCreateTableInfo {
    common::TableType type;
    std::string tableName;
    std::unique_ptr<BoundExtraCreateTableInfo> extraInfo;

    BoundCreateTableInfo(common::TableType type, std::string tableName,
        std::unique_ptr<BoundExtraCreateTableInfo> extraInfo)
        : type{type}, tableName{std::move(tableName)}, extraInfo{std::move(extraInfo)} {}
    BoundCreateTableInfo(const BoundCreateTableInfo& other)
        : type{other.type}, tableName{other.tableName}, extraInfo{other.extraInfo->copy()} {}

    inline std::unique_ptr<BoundCreateTableInfo> copy() const {
        return std::make_unique<BoundCreateTableInfo>(*this);
    }

    static std::vector<std::unique_ptr<BoundCreateTableInfo>> copy(
        const std::vector<std::unique_ptr<BoundCreateTableInfo>>& infos);
};

struct BoundExtraCreateNodeTableInfo : public BoundExtraCreateTableInfo {
    common::property_id_t primaryKeyIdx;
    std::vector<std::unique_ptr<catalog::Property>> properties;

    BoundExtraCreateNodeTableInfo(common::property_id_t primaryKeyIdx,
        std::vector<std::unique_ptr<catalog::Property>> properties)
        : primaryKeyIdx{primaryKeyIdx}, properties{std::move(properties)} {}
    BoundExtraCreateNodeTableInfo(const BoundExtraCreateNodeTableInfo& other)
        : primaryKeyIdx{other.primaryKeyIdx}, properties{
                                                  catalog::Property::copy(other.properties)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateNodeTableInfo>(*this);
    }
};

struct BoundExtraCreateRelTableInfo : public BoundExtraCreateTableInfo {
    catalog::RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    std::vector<std::unique_ptr<catalog::Property>> properties;

    BoundExtraCreateRelTableInfo(catalog::RelMultiplicity relMultiplicity,
        common::table_id_t srcTableID, common::table_id_t dstTableID,
        std::vector<std::unique_ptr<catalog::Property>> properties)
        : relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          properties{std::move(properties)} {}
    BoundExtraCreateRelTableInfo(const BoundExtraCreateRelTableInfo& other)
        : relMultiplicity{other.relMultiplicity}, srcTableID{other.srcTableID},
          dstTableID{other.dstTableID}, properties{catalog::Property::copy(other.properties)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateRelTableInfo>(*this);
    }
};

struct BoundExtraCreateRelTableGroupInfo : public BoundExtraCreateTableInfo {
    std::vector<std::unique_ptr<BoundCreateTableInfo>> infos;

    BoundExtraCreateRelTableGroupInfo(std::vector<std::unique_ptr<BoundCreateTableInfo>> infos)
        : infos{std::move(infos)} {}
    BoundExtraCreateRelTableGroupInfo(const BoundExtraCreateRelTableGroupInfo& other)
        : infos{BoundCreateTableInfo::copy(other.infos)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateRelTableGroupInfo>(*this);
    }
};

struct BoundExtraCreateRdfGraphInfo : public BoundExtraCreateTableInfo {
    std::unique_ptr<BoundCreateTableInfo> resourceInfo;
    std::unique_ptr<BoundCreateTableInfo> literalInfo;
    std::unique_ptr<BoundCreateTableInfo> resourceTripleInfo;
    std::unique_ptr<BoundCreateTableInfo> literalTripleInfo;

    BoundExtraCreateRdfGraphInfo(std::unique_ptr<BoundCreateTableInfo> resourceInfo,
        std::unique_ptr<BoundCreateTableInfo> literalInfo,
        std::unique_ptr<BoundCreateTableInfo> resourceTripleInfo,
        std::unique_ptr<BoundCreateTableInfo> literalTripleInfo)
        : resourceInfo{std::move(resourceInfo)}, literalInfo{std::move(literalInfo)},
          resourceTripleInfo{std::move(resourceTripleInfo)}, literalTripleInfo{
                                                                 std::move(literalTripleInfo)} {}
    BoundExtraCreateRdfGraphInfo(const BoundExtraCreateRdfGraphInfo& other)
        : resourceInfo{other.resourceInfo->copy()}, literalInfo{other.literalInfo->copy()},
          resourceTripleInfo{other.resourceTripleInfo->copy()},
          literalTripleInfo{other.literalTripleInfo->copy()} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateRdfGraphInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
