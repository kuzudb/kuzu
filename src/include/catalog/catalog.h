#pragma once

#include <memory>

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "catalog_content.h"

namespace kuzu {
namespace storage {
class WAL;
}
namespace transaction {
enum class TransactionAction : uint8_t;
class Transaction;
} // namespace transaction
namespace catalog {

class NodeTableCatalogEntry;
class RelTableCatalogEntry;
class RelGroupCatalogEntry;
class RDFGraphCatalogEntry;

class Catalog {
public:
    explicit Catalog(common::VirtualFileSystem* vfs);

    Catalog(storage::WAL* wal, common::VirtualFileSystem* vfs);

    // TODO(Guodong): Get rid of the following.
    inline CatalogContent* getReadOnlyVersion() const { return readOnlyVersion.get(); }

    // ----------------------------- Table Schemas ----------------------------
    uint64_t getTableCount(transaction::Transaction* tx) const;

    bool containsNodeTable(transaction::Transaction* tx) const;
    bool containsRelTable(transaction::Transaction* tx) const;
    bool containsTable(transaction::Transaction* tx, const std::string& tableName) const;

    common::table_id_t getTableID(transaction::Transaction* tx, const std::string& tableName) const;
    std::vector<common::table_id_t> getNodeTableIDs(transaction::Transaction* tx) const;
    std::vector<common::table_id_t> getRelTableIDs(transaction::Transaction* tx) const;

    std::string getTableName(transaction::Transaction* tx, common::table_id_t tableID) const;
    TableCatalogEntry* getTableCatalogEntry(
        transaction::Transaction* tx, common::table_id_t tableID) const;
    std::vector<NodeTableCatalogEntry*> getNodeTableEntries(transaction::Transaction* tx) const;
    std::vector<RelTableCatalogEntry*> getRelTableEntries(transaction::Transaction* tx) const;
    std::vector<RelGroupCatalogEntry*> getRelTableGroupEntries(transaction::Transaction* tx) const;
    std::vector<RDFGraphCatalogEntry*> getRdfGraphEntries(transaction::Transaction* tx) const;
    std::vector<TableCatalogEntry*> getTableEntries(transaction::Transaction* tx) const;
    std::vector<TableCatalogEntry*> getTableSchemas(
        transaction::Transaction* tx, const common::table_id_vector_t& tableIDs) const;
    CatalogSet* getFunctions(transaction::Transaction* tx) const;

    common::table_id_t addNodeTableSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRelTableSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRelTableGroupSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRdfGraphSchema(const binder::BoundCreateTableInfo& info);
    void dropTableSchema(common::table_id_t tableID);
    void renameTable(common::table_id_t tableID, const std::string& newName);

    void addNodeProperty(common::table_id_t tableID, const std::string& propertyName,
        std::unique_ptr<common::LogicalType> dataType);
    void addRelProperty(common::table_id_t tableID, const std::string& propertyName,
        std::unique_ptr<common::LogicalType> dataType);

    void dropProperty(common::table_id_t tableID, common::property_id_t propertyID);

    void renameProperty(
        common::table_id_t tableID, common::property_id_t propertyID, const std::string& newName);

    void setTableComment(common::table_id_t tableID, const std::string& comment);

    // ----------------------------- Functions ----------------------------
    common::ExpressionType getFunctionType(
        transaction::Transaction* tx, const std::string& name) const;
    void addFunction(std::string name, function::function_set functionSet);
    void addBuiltInFunction(std::string name, function::function_set functionSet);
    bool containsMacro(transaction::Transaction* tx, const std::string& macroName) const;
    void addScalarMacroFunction(
        std::string name, std::unique_ptr<function::ScalarMacroFunction> macro);
    // TODO(Ziyi): pass transaction pointer here.
    function::ScalarMacroFunction* getScalarMacroFunction(const std::string& name) const {
        return readOnlyVersion->getScalarMacroFunction(name);
    }

    std::vector<std::string> getMacroNames(transaction::Transaction* tx) const;

    // ----------------------------- Tx ----------------------------
    void prepareCommitOrRollback(transaction::TransactionAction action);
    void checkpointInMemory();

    void initCatalogContentForWriteTrxIfNecessary() {
        if (!readWriteVersion) {
            readWriteVersion = readOnlyVersion->copy();
        }
    }

    static void saveInitialCatalogToFile(
        const std::string& directory, common::VirtualFileSystem* vfs) {
        std::make_unique<Catalog>(vfs)->getReadOnlyVersion()->saveToFile(
            directory, common::FileVersionType::ORIGINAL);
    }

private:
    CatalogContent* getVersion(transaction::Transaction* tx) const;

    bool hasUpdates() const { return isUpdated; }

    void setToUpdated() { isUpdated = true; }
    void resetToNotUpdated() { isUpdated = false; }

protected:
    // The flat indicates if the readWriteVersion has been updated and is different from the
    // readOnlyVersion.
    bool isUpdated;
    std::unique_ptr<CatalogContent> readOnlyVersion;
    std::unique_ptr<CatalogContent> readWriteVersion;
    storage::WAL* wal;
};

} // namespace catalog
} // namespace kuzu
