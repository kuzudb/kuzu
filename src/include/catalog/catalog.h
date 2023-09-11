#pragma once

#include <memory>

#include "catalog_content.h"

namespace kuzu {
namespace transaction {
enum class TransactionAction : uint8_t;
}
namespace catalog {

class Catalog {
public:
    Catalog();

    explicit Catalog(storage::WAL* wal);

    // TODO(Guodong): Get rid of these two functions.
    inline CatalogContent* getReadOnlyVersion() const { return catalogContentForReadOnlyTrx.get(); }
    inline CatalogContent* getWriteVersion() const { return catalogContentForWriteTrx.get(); }

    inline function::BuiltInVectorFunctions* getBuiltInVectorFunctions() const {
        return catalogContentForReadOnlyTrx->builtInVectorFunctions.get();
    }
    inline function::BuiltInAggregateFunctions* getBuiltInAggregateFunction() const {
        return catalogContentForReadOnlyTrx->builtInAggregateFunctions.get();
    }
    inline function::BuiltInTableFunctions* getBuiltInTableFunction() const {
        return catalogContentForReadOnlyTrx->builtInTableFunctions.get();
    }

    void prepareCommitOrRollback(transaction::TransactionAction action);
    void checkpointInMemory();

    inline void initCatalogContentForWriteTrxIfNecessary() {
        if (!catalogContentForWriteTrx) {
            catalogContentForWriteTrx = catalogContentForReadOnlyTrx->copy();
        }
    }

    static inline void saveInitialCatalogToFile(const std::string& directory) {
        std::make_unique<Catalog>()->getReadOnlyVersion()->saveToFile(
            directory, common::DBFileType::ORIGINAL);
    }

    common::ExpressionType getFunctionType(const std::string& name) const;

    common::table_id_t addNodeTableSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRelTableSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRelTableGroupSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRdfGraphSchema(const binder::BoundCreateTableInfo& info);

    void dropTableSchema(common::table_id_t tableID);

    void renameTable(common::table_id_t tableID, const std::string& newName);

    void addNodeProperty(common::table_id_t tableID, const std::string& propertyName,
        std::unique_ptr<common::LogicalType> dataType,
        std::unique_ptr<MetadataDAHInfo> metadataDAHInfo);
    void addRelProperty(common::table_id_t tableID, const std::string& propertyName,
        std::unique_ptr<common::LogicalType> dataType);

    void dropProperty(common::table_id_t tableID, common::property_id_t propertyID);

    void renameProperty(
        common::table_id_t tableID, common::property_id_t propertyID, const std::string& newName);

    std::unordered_set<TableSchema*> getAllRelTableSchemasContainBoundTable(
        common::table_id_t boundTableID) const;

    void addVectorFunction(std::string name, function::vector_function_definitions definitions);

    void addScalarMacroFunction(
        std::string name, std::unique_ptr<function::ScalarMacroFunction> macro);

    // TODO(Ziyi): pass transaction pointer here.
    inline function::ScalarMacroFunction* getScalarMacroFunction(const std::string& name) const {
        return catalogContentForReadOnlyTrx->macros.at(name).get();
    }

private:
    inline bool hasUpdates() { return catalogContentForWriteTrx != nullptr; }

protected:
    std::unique_ptr<CatalogContent> catalogContentForReadOnlyTrx;
    std::unique_ptr<CatalogContent> catalogContentForWriteTrx;
    storage::WAL* wal;
};

} // namespace catalog
} // namespace kuzu
