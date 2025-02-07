#pragma once

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "common/enums/conflict_action.h"
#include "common/enums/extend_direction.h"
#include "common/enums/rel_multiplicity.h"
#include "common/types/types.h"
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

    virtual inline std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const = 0;
};

struct BoundCreateTableInfo {
    catalog::CatalogEntryType type = catalog::CatalogEntryType::DUMMY_ENTRY;
    std::string tableName;
    common::ConflictAction onConflict = common::ConflictAction::INVALID;
    bool hasParent = false;
    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> extraInfo;
    bool isInternal = false;

    BoundCreateTableInfo() = default;
    BoundCreateTableInfo(catalog::CatalogEntryType type, std::string tableName,
        common::ConflictAction onConflict,
        std::unique_ptr<BoundExtraCreateCatalogEntryInfo> extraInfo, bool isInternal)
        : type{type}, tableName{std::move(tableName)}, onConflict{onConflict},
          extraInfo{std::move(extraInfo)}, isInternal{isInternal} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateTableInfo);

    std::string toString() const;

private:
    BoundCreateTableInfo(const BoundCreateTableInfo& other)
        : type{other.type}, tableName{other.tableName}, onConflict{other.onConflict},
          hasParent{other.hasParent}, extraInfo{other.extraInfo->copy()},
          isInternal{other.isInternal} {}
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
};

struct BoundExtraCreateNodeTableInfo final : BoundExtraCreateTableInfo {
    std::string primaryKeyName;

    BoundExtraCreateNodeTableInfo(std::string primaryKeyName,
        std::vector<PropertyDefinition> definitions)
        : BoundExtraCreateTableInfo{std::move(definitions)},
          primaryKeyName{std::move(primaryKeyName)} {}
    BoundExtraCreateNodeTableInfo(const BoundExtraCreateNodeTableInfo& other)
        : BoundExtraCreateTableInfo{copyVector(other.propertyDefinitions)},
          primaryKeyName{other.primaryKeyName} {}

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateNodeTableInfo>(*this);
    }
};

struct BoundExtraCreateRelTableInfo final : BoundExtraCreateTableInfo {
    common::RelMultiplicity srcMultiplicity;
    common::RelMultiplicity dstMultiplicity;
    common::ExtendDirection storageDirection;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;

    BoundExtraCreateRelTableInfo(common::table_id_t srcTableID, common::table_id_t dstTableID,
        std::vector<PropertyDefinition> definitions);
    BoundExtraCreateRelTableInfo(common::RelMultiplicity srcMultiplicity,
        common::RelMultiplicity dstMultiplicity, common::ExtendDirection storageDirection,
        common::table_id_t srcTableID, common::table_id_t dstTableID,
        std::vector<PropertyDefinition> definitions);
    BoundExtraCreateRelTableInfo(const BoundExtraCreateRelTableInfo& other);

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateRelTableInfo>(*this);
    }
};

struct BoundExtraCreateRelTableGroupInfo final : BoundExtraCreateCatalogEntryInfo {
    std::vector<BoundCreateTableInfo> infos;

    explicit BoundExtraCreateRelTableGroupInfo(std::vector<BoundCreateTableInfo> infos)
        : infos{std::move(infos)} {}
    BoundExtraCreateRelTableGroupInfo(const BoundExtraCreateRelTableGroupInfo& other)
        : infos{copyVector(other.infos)} {}

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateRelTableGroupInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
