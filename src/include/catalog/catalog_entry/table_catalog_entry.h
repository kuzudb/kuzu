#pragma once

#include <vector>

#include "binder/ddl/bound_alter_info.h"
#include "binder/ddl/bound_create_table_info.h"
#include "catalog/property.h"
#include "catalog_entry.h"
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
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    TableCatalogEntry() = default;
    TableCatalogEntry(CatalogSet* set, CatalogEntryType catalogType, std::string name,
        common::table_id_t tableID)
        : CatalogEntry{catalogType, std::move(name)}, set{set}, tableID{tableID}, nextPID{0},
          nextColumnID{0} {}
    TableCatalogEntry& operator=(const TableCatalogEntry&) = delete;

    std::unique_ptr<TableCatalogEntry> alter(const binder::BoundAlterInfo& alterInfo);

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    common::table_id_t getTableID() const { return tableID; }
    std::string getComment() const { return comment; }
    void setComment(std::string newComment) { comment = std::move(newComment); }
    virtual bool isParent(common::table_id_t /*tableID*/) { return false; };
    // TODO(Guodong/Ziyi): This function should be removed. Instead we should use CatalogEntryType.
    virtual common::TableType getTableType() const = 0;
    virtual function::TableFunction getScanFunction() { KU_UNREACHABLE; }
    binder::BoundAlterInfo* getAlterInfo() const { return alterInfo.get(); }
    void setAlterInfo(const binder::BoundAlterInfo& alterInfo_) {
        alterInfo = std::make_unique<binder::BoundAlterInfo>(alterInfo_.copy());
    }

    //===--------------------------------------------------------------------===//
    // properties functions
    //===--------------------------------------------------------------------===//
    uint32_t getNumProperties() const { return properties.size(); }
    const std::vector<Property>& getPropertiesRef() const { return properties; }
    bool containProperty(const std::string& propertyName) const;
    common::property_id_t getPropertyID(const std::string& propertyName) const;
    const Property* getProperty(common::property_id_t propertyID) const;
    uint32_t getPropertyPos(common::property_id_t propertyID) const;
    virtual common::column_id_t getColumnID(common::property_id_t propertyID) const;
    bool containPropertyType(const common::LogicalType& logicalType) const;
    void addProperty(std::string propertyName, common::LogicalType dataType,
        std::unique_ptr<parser::ParsedExpression> defaultExpr);
    void dropProperty(common::property_id_t propertyID);
    void renameProperty(common::property_id_t propertyID, const std::string& newName);

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
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
    common::table_id_t tableID;
    std::string comment;
    common::property_id_t nextPID;
    common::column_id_t nextColumnID;
    std::vector<Property> properties;
    std::unique_ptr<binder::BoundAlterInfo> alterInfo;
};

} // namespace catalog
} // namespace kuzu
