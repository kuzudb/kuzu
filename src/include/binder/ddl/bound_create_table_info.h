#pragma once

#include "common/copy_constructors.h"
#include "common/enums/table_type.h"
#include "common/types/types.h"

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

private:
    BoundCreateTableInfo(const BoundCreateTableInfo& other)
        : type{other.type}, tableName{other.tableName}, extraInfo{other.extraInfo->copy()} {}
};

struct PropertyInfo {
    std::string name;
    common::LogicalType type;

    PropertyInfo(std::string name, common::LogicalType type)
        : name{std::move(name)}, type{std::move(type)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(PropertyInfo);

private:
    PropertyInfo(const PropertyInfo& other) : name{other.name}, type{other.type} {}
};

struct BoundExtraCreateNodeTableInfo : public BoundExtraCreateTableInfo {
    common::property_id_t primaryKeyIdx;
    std::vector<PropertyInfo> propertyInfos;

    BoundExtraCreateNodeTableInfo(
        common::property_id_t primaryKeyIdx, std::vector<PropertyInfo> propertyInfos)
        : primaryKeyIdx{primaryKeyIdx}, propertyInfos{std::move(propertyInfos)} {}
    BoundExtraCreateNodeTableInfo(const BoundExtraCreateNodeTableInfo& other)
        : primaryKeyIdx{other.primaryKeyIdx}, propertyInfos{copyVector(other.propertyInfos)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateNodeTableInfo>(*this);
    }
};

struct BoundExtraCreateRelTableInfo : public BoundExtraCreateTableInfo {
    catalog::RelMultiplicity srcMultiplicity;
    catalog::RelMultiplicity dstMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    std::vector<PropertyInfo> propertyInfos;

    BoundExtraCreateRelTableInfo(catalog::RelMultiplicity srcMultiplicity,
        catalog::RelMultiplicity dstMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID, std::vector<PropertyInfo> propertyInfos)
        : srcMultiplicity{srcMultiplicity}, dstMultiplicity{dstMultiplicity},
          srcTableID{srcTableID}, dstTableID{dstTableID}, propertyInfos{std::move(propertyInfos)} {}
    BoundExtraCreateRelTableInfo(const BoundExtraCreateRelTableInfo& other)
        : srcMultiplicity{other.srcMultiplicity}, dstMultiplicity{other.dstMultiplicity},
          srcTableID{other.srcTableID}, dstTableID{other.dstTableID}, propertyInfos{copyVector(
                                                                          other.propertyInfos)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateRelTableInfo>(*this);
    }
};

struct BoundExtraCreateRelTableGroupInfo : public BoundExtraCreateTableInfo {
    std::vector<BoundCreateTableInfo> infos;

    explicit BoundExtraCreateRelTableGroupInfo(std::vector<BoundCreateTableInfo> infos)
        : infos{std::move(infos)} {}
    BoundExtraCreateRelTableGroupInfo(const BoundExtraCreateRelTableGroupInfo& other)
        : infos{copyVector(other.infos)} {}

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
