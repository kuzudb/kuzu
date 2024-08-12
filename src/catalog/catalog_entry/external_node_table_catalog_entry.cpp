#include "catalog/catalog_entry/external_node_table_catalog_entry.h"

#include "common/serializer/deserializer.h"
#include "binder/ddl/bound_create_table_info.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

void ExternalNodeTableCatalogEntry::serialize(Serializer& serializer) const {
    ExternalTableCatalogEntry::serialize(serializer);
    serializer.write(primaryKeyName);
}

std::unique_ptr<ExternalNodeTableCatalogEntry> ExternalNodeTableCatalogEntry::deserialize(Deserializer& deserializer) {
    std::string primaryKeyName;
    deserializer.deserializeValue(primaryKeyName);
    auto entry = std::make_unique<ExternalNodeTableCatalogEntry>();
    entry->primaryKeyName = primaryKeyName;
    return entry;
}

std::unique_ptr<TableCatalogEntry> ExternalNodeTableCatalogEntry::copy() const {
    auto result = std::make_unique<ExternalNodeTableCatalogEntry>();
    result->primaryKeyName = primaryKeyName;
    result->copyFrom(*this);
    return result;
}

std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> ExternalNodeTableCatalogEntry::getBoundExtraCreateInfo(Transaction* transaction) const {
    return std::make_unique<BoundExtraCreateExternalNodeTableInfo>(primaryKeyName, externalDBName,
        externalTableName, physicalEntry->getBoundCreateTableInfo(transaction), copyVector(getProperties()));
}

}
}
