#pragma once

#include <memory>

#include "catalog_content.h"

namespace kuzu {
namespace storage {
class WAL;
}
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

    inline function::BuiltInFunctions* getBuiltInFunctions() const {
        return catalogContentForReadOnlyTrx->builtInFunctions.get();
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

    void addScalarMacroFunction(
        std::string name, std::unique_ptr<function::ScalarMacroFunction> macro);

    void setTableComment(common::table_id_t tableID, const std::string& comment);

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
