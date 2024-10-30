#pragma once

#include "common/enums/conflict_action.h"
#include "common/enums/rel_multiplicity.h"
#include "common/enums/table_type.h"
#include "common/types/types.h"
#include "common/types/value/value.h"
#include "property_definition.h"

namespace kuzu {
namespace common {
enum class RelMultiplicity : uint8_t;
}
namespace binder {
struct BoundExtraCreateCatalogEntryInfo {
    virtual ~BoundExtraCreateCatalogEntryInfo() = default;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TARGET*>(this);
    }

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }

    virtual void serialize(common::Serializer& serializer) const = 0;

    virtual inline std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const = 0;
};

struct BoundCreateTableInfo {
    common::TableType type{};
    std::string tableName;
    common::ConflictAction onConflict = common::ConflictAction::INVALID;
    bool hasParent = false;
    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> extraInfo;

    BoundCreateTableInfo() = default;
    BoundCreateTableInfo(common::TableType type, std::string tableName,
        common::ConflictAction onConflict,
        std::unique_ptr<BoundExtraCreateCatalogEntryInfo> extraInfo)
        : type{type}, tableName{std::move(tableName)}, onConflict{onConflict},
          extraInfo{std::move(extraInfo)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateTableInfo);

    std::string toString() const;

    void serialize(common::Serializer& serializer) const;
    static BoundCreateTableInfo deserialize(common::Deserializer& deserializer);

private:
    BoundCreateTableInfo(const BoundCreateTableInfo& other)
        : type{other.type}, tableName{other.tableName}, onConflict{other.onConflict},
          hasParent{other.hasParent}, extraInfo{other.extraInfo->copy()} {}
};

struct KUZU_API BoundExtraCreateTableInfo : public BoundExtraCreateCatalogEntryInfo {
    std::vector<PropertyDefinition> propertyDefinitions;

    explicit BoundExtraCreateTableInfo(std::vector<PropertyDefinition> propertyDefinitions)
        : propertyDefinitions{std::move(propertyDefinitions)} {}

    BoundExtraCreateTableInfo(const BoundExtraCreateTableInfo& other)
        : BoundExtraCreateTableInfo{copyVector(other.propertyDefinitions)} {}
    BoundExtraCreateTableInfo& operator=(const BoundExtraCreateTableInfo&) = delete;

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateTableInfo>(*this);
    }

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<BoundExtraCreateTableInfo> deserialize(
        common::Deserializer& deserializer, common::TableType type);
};

struct BoundExtraCreateNodeTableInfo final : public BoundExtraCreateTableInfo {
    std::string primaryKeyName;

    BoundExtraCreateNodeTableInfo(const std::string& primaryKeyName,
        std::vector<PropertyDefinition> definitions)
        : BoundExtraCreateTableInfo{std::move(definitions)}, primaryKeyName{primaryKeyName} {}
    BoundExtraCreateNodeTableInfo(const BoundExtraCreateNodeTableInfo& other)
        : BoundExtraCreateTableInfo{copyVector(other.propertyDefinitions)},
          primaryKeyName{other.primaryKeyName} {}

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
        std::vector<PropertyDefinition> definitions)
        : BoundExtraCreateRelTableInfo{common::RelMultiplicity::MANY, common::RelMultiplicity::MANY,
              srcTableID, dstTableID, std::move(definitions)} {}
    BoundExtraCreateRelTableInfo(common::RelMultiplicity srcMultiplicity,
        common::RelMultiplicity dstMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID, std::vector<PropertyDefinition> definitions)
        : BoundExtraCreateTableInfo{std::move(definitions)}, srcMultiplicity{srcMultiplicity},
          dstMultiplicity{dstMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {}
    BoundExtraCreateRelTableInfo(const BoundExtraCreateRelTableInfo& other)
        : BoundExtraCreateTableInfo{copyVector(other.propertyDefinitions)},
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

} // namespace binder
} // namespace kuzu
