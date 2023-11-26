#pragma once

#include <memory>

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

class Catalog {
public:
    Catalog();

    explicit Catalog(storage::WAL* wal);

    // TODO(Guodong): Get rid of the following.
    inline CatalogContent* getReadOnlyVersion() const { return readOnlyVersion.get(); }

    uint64_t getTableCount(transaction::Transaction* tx) const;

    bool containsNodeTable(transaction::Transaction* tx) const;
    bool containsRelTable(transaction::Transaction* tx) const;
    bool containsRdfGraph(transaction::Transaction* tx) const;
    bool containsTable(transaction::Transaction* tx, const std::string& tableName) const;

    common::table_id_t getTableID(transaction::Transaction* tx, const std::string& tableName) const;
    std::vector<common::table_id_t> getNodeTableIDs(transaction::Transaction* tx) const;
    std::vector<common::table_id_t> getRelTableIDs(transaction::Transaction* tx) const;
    std::vector<common::table_id_t> getRdfGraphIDs(transaction::Transaction* tx) const;

    std::string getTableName(transaction::Transaction* tx, common::table_id_t tableID) const;
    TableSchema* getTableSchema(transaction::Transaction* tx, common::table_id_t tableID) const;
    std::vector<TableSchema*> getNodeTableSchemas(transaction::Transaction* tx) const;
    std::vector<TableSchema*> getRelTableSchemas(transaction::Transaction* tx) const;
    std::vector<TableSchema*> getRelTableGroupSchemas(transaction::Transaction* tx) const;
    std::vector<TableSchema*> getTableSchemas(transaction::Transaction* tx) const;
    std::vector<TableSchema*> getTableSchemas(
        transaction::Transaction* tx, const common::table_id_vector_t& tableIDs) const;

    inline function::BuiltInFunctions* getBuiltInFunctions() const {
        return readOnlyVersion->builtInFunctions.get();
    }
    void prepareCommitOrRollback(transaction::TransactionAction action);
    void checkpointInMemory();

    inline void initCatalogContentForWriteTrxIfNecessary() {
        if (!readWriteVersion) {
            readWriteVersion = readOnlyVersion->copy();
        }
    }

    static inline void saveInitialCatalogToFile(const std::string& directory) {
        std::make_unique<Catalog>()->getReadOnlyVersion()->saveToFile(
            directory, common::FileVersionType::ORIGINAL);
    }

    common::ExpressionType getFunctionType(const std::string& name) const;

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

    void addFunction(std::string name, function::function_set functionSet);
    bool containsMacro(transaction::Transaction* tx, const std::string& macroName) const;
    void addScalarMacroFunction(
        std::string name, std::unique_ptr<function::ScalarMacroFunction> macro);

    void setTableComment(common::table_id_t tableID, const std::string& comment);

    // TODO(Ziyi): pass transaction pointer here.
    inline function::ScalarMacroFunction* getScalarMacroFunction(const std::string& name) const {
        return readOnlyVersion->macros.at(name).get();
    }

private:
    inline CatalogContent* getVersion(transaction::Transaction* tx) const;

    inline bool hasUpdates() { return readWriteVersion != nullptr; }

protected:
    std::unique_ptr<CatalogContent> readOnlyVersion;
    std::unique_ptr<CatalogContent> readWriteVersion;
    storage::WAL* wal;
};

} // namespace catalog
} // namespace kuzu
