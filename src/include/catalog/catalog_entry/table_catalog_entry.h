#pragma once

#include <vector>

#include "binder/ddl/bound_alter_info.h"
#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_entry/catalog_entry.h"
#include "catalog/property_definition_collection.h"
#include "common/enums/table_type.h"
#include "function/table_functions.h"

namespace kuzu {
namespace binder {
struct BoundExtraCreateCatalogEntryInfo;
} // namespace binder

namespace transaction {
class Transaction;
} // namespace transaction

namespace catalog {

class CatalogSet;
class KUZU_API TableCatalogEntry : public CatalogEntry {
public:
    TableCatalogEntry() : set{nullptr} {}
    TableCatalogEntry(CatalogSet* set, CatalogEntryType catalogType, std::string name)
        : CatalogEntry{catalogType, std::move(name)}, set{set} {}
    TableCatalogEntry& operator=(const TableCatalogEntry&) = delete;

    common::table_id_t getTableID() const { return oid; }

    std::unique_ptr<TableCatalogEntry> alter(const binder::BoundAlterInfo& alterInfo) const;

    virtual bool isParent(common::table_id_t /*tableID*/) { return false; };
    virtual common::TableType getTableType() const = 0;

    std::string getComment() const { return comment; }
    void setComment(std::string newComment) { comment = std::move(newComment); }

    virtual function::TableFunction getScanFunction() { KU_UNREACHABLE; }

    binder::BoundAlterInfo* getAlterInfo() const { return alterInfo.get(); }
    void resetAlterInfo() { alterInfo = nullptr; }
    void setAlterInfo(const binder::BoundAlterInfo& alterInfo_) {
        alterInfo = std::make_unique<binder::BoundAlterInfo>(alterInfo_.copy());
    }

    common::column_id_t getMaxColumnID() const;
    void vacuumColumnIDs(common::column_id_t nextColumnID);
    std::string propertiesToCypher() const;
    const std::vector<binder::PropertyDefinition>& getProperties() const {
        return propertyCollection.getDefinitions();
    }
    common::idx_t getNumProperties() const { return propertyCollection.size(); }
    bool containsProperty(const std::string& propertyName) const;
    common::idx_t getPropertyIdx(const std::string& propertyName) const;
    const binder::PropertyDefinition& getProperty(const std::string& propertyName) const;
    const binder::PropertyDefinition& getProperty(common::idx_t idx) const;
    virtual common::column_id_t getColumnID(const std::string& propertyName) const;
    void addProperty(const binder::PropertyDefinition& propertyDefinition);
    void dropProperty(const std::string& propertyName);
    void renameProperty(const std::string& propertyName, const std::string& newName);

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<TableCatalogEntry> deserialize(common::Deserializer& deserializer,
        CatalogEntryType type);
    virtual std::unique_ptr<TableCatalogEntry> copy() const = 0;

    binder::BoundCreateTableInfo getBoundCreateTableInfo(
        transaction::Transaction* transaction) const;

protected:
    void copyFrom(const CatalogEntry& other) override;
    virtual std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction* transaction) const = 0;

protected:
    CatalogSet* set;
    std::string comment;
    PropertyDefinitionCollection propertyCollection;
    std::unique_ptr<binder::BoundAlterInfo> alterInfo;
};

struct TableCatalogEntryHasher {
    std::size_t operator()(TableCatalogEntry* entry) const {
        return std::hash<common::table_id_t>{}(entry->getTableID());
    }
};

struct TableCatalogEntryEquality {
    bool operator()(TableCatalogEntry* left, TableCatalogEntry* right) const {
        return left->getTableID() == right->getTableID();
    }
};

using table_catalog_entry_set_t =
    std::unordered_set<TableCatalogEntry*, TableCatalogEntryHasher, TableCatalogEntryEquality>;

} // namespace catalog
} // namespace kuzu
