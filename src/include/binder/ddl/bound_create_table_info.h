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
    EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateTableInfo);

    static std::vector<BoundCreateTableInfo> copy(const std::vector<BoundCreateTableInfo>& infos);

private:
    BoundCreateTableInfo(const BoundCreateTableInfo& other)
        : type{other.type}, tableName{other.tableName}, extraInfo{other.extraInfo->copy()} {}
};

struct BoundExtraCreateNodeTableInfo : public BoundExtraCreateTableInfo {
    common::property_id_t primaryKeyIdx;
    std::vector<catalog::Property> properties;

    BoundExtraCreateNodeTableInfo(
        common::property_id_t primaryKeyIdx, std::vector<catalog::Property> properties)
        : primaryKeyIdx{primaryKeyIdx}, properties{std::move(properties)} {}
    BoundExtraCreateNodeTableInfo(const BoundExtraCreateNodeTableInfo& other)
        : primaryKeyIdx{other.primaryKeyIdx}, properties{
                                                  catalog::Property::copy(other.properties)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateNodeTableInfo>(*this);
    }
};

struct BoundExtraCreateRelTableInfo : public BoundExtraCreateTableInfo {
    catalog::RelMultiplicity srcMultiplicity;
    catalog::RelMultiplicity dstMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    std::vector<catalog::Property> properties;

    BoundExtraCreateRelTableInfo(catalog::RelMultiplicity srcMultiplicity,
        catalog::RelMultiplicity dstMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID, std::vector<catalog::Property> properties)
        : srcMultiplicity{srcMultiplicity}, dstMultiplicity{dstMultiplicity},
          srcTableID{srcTableID}, dstTableID{dstTableID}, properties{std::move(properties)} {}
    BoundExtraCreateRelTableInfo(const BoundExtraCreateRelTableInfo& other)
        : srcMultiplicity{other.srcMultiplicity}, dstMultiplicity{other.dstMultiplicity},
          srcTableID{other.srcTableID}, dstTableID{other.dstTableID}, properties{
                                                                          catalog::Property::copy(
                                                                              other.properties)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateRelTableInfo>(*this);
    }
};

struct BoundExtraCreateRelTableGroupInfo : public BoundExtraCreateTableInfo {
    std::vector<BoundCreateTableInfo> infos;

    explicit BoundExtraCreateRelTableGroupInfo(std::vector<BoundCreateTableInfo> infos)
        : infos{std::move(infos)} {}
    BoundExtraCreateRelTableGroupInfo(const BoundExtraCreateRelTableGroupInfo& other)
        : infos{BoundCreateTableInfo::copy(other.infos)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateRelTableGroupInfo>(*this);
    }
};

struct BoundExtraCreateRdfGraphInfo : public BoundExtraCreateTableInfo {
    BoundCreateTableInfo resourceInfo;
    BoundCreateTableInfo literalInfo;
    BoundCreateTableInfo resourceTripleInfo;
    BoundCreateTableInfo literalTripleInfo;

    BoundExtraCreateRdfGraphInfo(BoundCreateTableInfo resourceInfo,
        BoundCreateTableInfo literalInfo, BoundCreateTableInfo resourceTripleInfo,
        BoundCreateTableInfo literalTripleInfo)
        : resourceInfo{std::move(resourceInfo)}, literalInfo{std::move(literalInfo)},
          resourceTripleInfo{std::move(resourceTripleInfo)}, literalTripleInfo{
                                                                 std::move(literalTripleInfo)} {}
    BoundExtraCreateRdfGraphInfo(const BoundExtraCreateRdfGraphInfo& other)
        : resourceInfo{other.resourceInfo.copy()}, literalInfo{other.literalInfo.copy()},
          resourceTripleInfo{other.resourceTripleInfo.copy()},
          literalTripleInfo{other.literalTripleInfo.copy()} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateRdfGraphInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
