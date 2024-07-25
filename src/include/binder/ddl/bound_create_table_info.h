#pragma once

#include "common/copy_constructors.h"
#include "common/enums/conflict_action.h"
#include "common/enums/rel_multiplicity.h"
#include "common/enums/table_type.h"
#include "common/types/types.h"
#include "common/types/value/value.h"
#include "parser/expression/parsed_literal_expression.h"

namespace kuzu {
namespace common {
enum class RelMultiplicity : uint8_t;
}
namespace binder {
struct BoundExtraCreateCatalogEntryInfo {
    virtual ~BoundExtraCreateCatalogEntryInfo() = default;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const BoundExtraCreateCatalogEntryInfo*, const TARGET*>(
            this);
    }

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<BoundExtraCreateCatalogEntryInfo*, TARGET*>(this);
    }

    virtual void serialize(common::Serializer& serializer) const = 0;

    virtual inline std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const = 0;
};

struct BoundCreateTableInfo {
    common::TableType type;
    std::string tableName;
    common::ConflictAction onConflict;
    bool hasParent = false;
    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> extraInfo;

    BoundCreateTableInfo() = default;
    BoundCreateTableInfo(common::TableType type, std::string tableName,
        common::ConflictAction onConflict,
        std::unique_ptr<BoundExtraCreateCatalogEntryInfo> extraInfo)
        : type{type}, tableName{std::move(tableName)}, onConflict{onConflict},
          extraInfo{std::move(extraInfo)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateTableInfo);

    void serialize(common::Serializer& serializer) const;
    static BoundCreateTableInfo deserialize(common::Deserializer& deserializer);

private:
    BoundCreateTableInfo(const BoundCreateTableInfo& other)
        : type{other.type}, tableName{other.tableName}, onConflict{other.onConflict},
          hasParent{other.hasParent}, extraInfo{other.extraInfo->copy()} {}
};

struct PropertyInfo {
    std::string name;
    common::LogicalType type;
    std::unique_ptr<parser::ParsedExpression> defaultValue;

    PropertyInfo() = default;
    PropertyInfo(std::string name, common::LogicalType type)
        : PropertyInfo{name, std::move(type),
              std::make_unique<parser::ParsedLiteralExpression>(common::Value::createNullValue(),
                  "NULL")} {}

    PropertyInfo(std::string name, common::LogicalType type,
        std::unique_ptr<parser::ParsedExpression> defaultValue)
        : name{std::move(name)}, type{std::move(type)}, defaultValue{std::move(defaultValue)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(PropertyInfo);

    void serialize(common::Serializer& serializer) const;
    static PropertyInfo deserialize(common::Deserializer& deserializer);

private:
    PropertyInfo(const PropertyInfo& other)
        : name{other.name}, type{other.type.copy()}, defaultValue{other.defaultValue->copy()} {}
};

struct KUZU_API BoundExtraCreateTableInfo : public BoundExtraCreateCatalogEntryInfo {
    std::vector<PropertyInfo> propertyInfos;

    explicit BoundExtraCreateTableInfo(std::vector<PropertyInfo> propertyInfos)
        : propertyInfos{std::move(propertyInfos)} {}

    BoundExtraCreateTableInfo(const BoundExtraCreateTableInfo& other)
        : BoundExtraCreateTableInfo{copyVector(other.propertyInfos)} {}
    BoundExtraCreateTableInfo& operator=(const BoundExtraCreateTableInfo&) = delete;

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateTableInfo>(*this);
    }

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<BoundExtraCreateTableInfo> deserialize(
        common::Deserializer& deserializer, common::TableType type);
};

struct BoundExtraCreateNodeTableInfo final : public BoundExtraCreateTableInfo {
    common::property_id_t primaryKeyIdx;

    BoundExtraCreateNodeTableInfo(common::property_id_t primaryKeyIdx,
        std::vector<PropertyInfo> propertyInfos)
        : BoundExtraCreateTableInfo{std::move(propertyInfos)}, primaryKeyIdx{primaryKeyIdx} {}
    BoundExtraCreateNodeTableInfo(const BoundExtraCreateNodeTableInfo& other)
        : BoundExtraCreateTableInfo{copyVector(other.propertyInfos)},
          primaryKeyIdx{other.primaryKeyIdx} {}

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateNodeTableInfo>(*this);
    }

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<BoundExtraCreateNodeTableInfo> deserialize(
        common::Deserializer& deserializer);
};

struct BoundExtraCreateRelTableInfo final : public BoundExtraCreateTableInfo {
    common::RelMultiplicity srcMultiplicity;
    common::RelMultiplicity dstMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;

    BoundExtraCreateRelTableInfo(common::table_id_t srcTableID, common::table_id_t dstTableID,
        std::vector<PropertyInfo> propertyInfos)
        : BoundExtraCreateRelTableInfo{common::RelMultiplicity::MANY, common::RelMultiplicity::MANY,
              srcTableID, dstTableID, std::move(propertyInfos)} {}
    BoundExtraCreateRelTableInfo(common::RelMultiplicity srcMultiplicity,
        common::RelMultiplicity dstMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID, std::vector<PropertyInfo> propertyInfos)
        : BoundExtraCreateTableInfo{std::move(propertyInfos)}, srcMultiplicity{srcMultiplicity},
          dstMultiplicity{dstMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {}
    BoundExtraCreateRelTableInfo(const BoundExtraCreateRelTableInfo& other)
        : BoundExtraCreateTableInfo{copyVector(other.propertyInfos)},
          srcMultiplicity{other.srcMultiplicity}, dstMultiplicity{other.dstMultiplicity},
          srcTableID{other.srcTableID}, dstTableID{other.dstTableID} {}

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateRelTableInfo>(*this);
    }

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<BoundExtraCreateRelTableInfo> deserialize(
        common::Deserializer& deserializer);
};

struct BoundExtraCreateRelTableGroupInfo final : public BoundExtraCreateCatalogEntryInfo {
    std::vector<BoundCreateTableInfo> infos;

    explicit BoundExtraCreateRelTableGroupInfo(std::vector<BoundCreateTableInfo> infos)
        : infos{std::move(infos)} {}
    BoundExtraCreateRelTableGroupInfo(const BoundExtraCreateRelTableGroupInfo& other)
        : infos{copyVector(other.infos)} {}

    inline std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateRelTableGroupInfo>(*this);
    }

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<BoundExtraCreateRelTableGroupInfo> deserialize(
        common::Deserializer& deserializer);
};

struct BoundExtraCreateRdfGraphInfo final : public BoundExtraCreateCatalogEntryInfo {
    BoundCreateTableInfo resourceInfo;
    BoundCreateTableInfo literalInfo;
    BoundCreateTableInfo resourceTripleInfo;
    BoundCreateTableInfo literalTripleInfo;

    BoundExtraCreateRdfGraphInfo(BoundCreateTableInfo resourceInfo,
        BoundCreateTableInfo literalInfo, BoundCreateTableInfo resourceTripleInfo,
        BoundCreateTableInfo literalTripleInfo)
        : resourceInfo{std::move(resourceInfo)}, literalInfo{std::move(literalInfo)},
          resourceTripleInfo{std::move(resourceTripleInfo)},
          literalTripleInfo{std::move(literalTripleInfo)} {}
    BoundExtraCreateRdfGraphInfo(const BoundExtraCreateRdfGraphInfo& other)
        : resourceInfo{other.resourceInfo.copy()}, literalInfo{other.literalInfo.copy()},
          resourceTripleInfo{other.resourceTripleInfo.copy()},
          literalTripleInfo{other.literalTripleInfo.copy()} {}

    inline std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateRdfGraphInfo>(*this);
    }

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<BoundExtraCreateRdfGraphInfo> deserialize(
        common::Deserializer& deserializer);
};

} // namespace binder
} // namespace kuzu
