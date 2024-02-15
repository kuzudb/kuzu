#include "catalog/rel_table_schema.h"

#include <sstream>

#include "catalog/catalog.h"
#include "catalog/export_utils.h"
#include "common/exception/binder.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::main;

namespace kuzu {
namespace catalog {

RelMultiplicity RelMultiplicityUtils::getFwd(const std::string& multiplicityStr) {
    if ("ONE_ONE" == multiplicityStr || "ONE_MANY" == multiplicityStr) {
        return RelMultiplicity::ONE;
    } else if ("MANY_ONE" == multiplicityStr || "MANY_MANY" == multiplicityStr) {
        return RelMultiplicity::MANY;
    }
    throw BinderException(
        stringFormat("Cannot bind {} as relationship multiplicity.", multiplicityStr));
}

RelMultiplicity RelMultiplicityUtils::getBwd(const std::string& multiplicityStr) {
    if ("ONE_ONE" == multiplicityStr || "MANY_ONE" == multiplicityStr) {
        return RelMultiplicity::ONE;
    } else if ("ONE_MANY" == multiplicityStr || "MANY_MANY" == multiplicityStr) {
        return RelMultiplicity::MANY;
    }
    throw BinderException(
        stringFormat("Cannot bind {} as relationship multiplicity.", multiplicityStr));
}

RelTableSchema::RelTableSchema(const RelTableSchema& other) : TableSchema{other} {
    srcMultiplicity = other.srcMultiplicity;
    dstMultiplicity = other.dstMultiplicity;
    srcTableID = other.srcTableID;
    dstTableID = other.dstTableID;
}

void RelTableSchema::serializeInternal(Serializer& serializer) {
    serializer.serializeValue(srcMultiplicity);
    serializer.serializeValue(dstMultiplicity);
    serializer.serializeValue(srcTableID);
    serializer.serializeValue(dstTableID);
}

std::unique_ptr<RelTableSchema> RelTableSchema::deserialize(Deserializer& deserializer) {
    RelMultiplicity srcMultiplicity;
    RelMultiplicity dstMultiplicity;
    table_id_t srcTableID;
    table_id_t dstTableID;
    deserializer.deserializeValue(srcMultiplicity);
    deserializer.deserializeValue(dstMultiplicity);
    deserializer.deserializeValue(srcTableID);
    deserializer.deserializeValue(dstTableID);
    auto schema = std::make_unique<RelTableSchema>();
    schema->srcMultiplicity = srcMultiplicity;
    schema->dstMultiplicity = dstMultiplicity;
    schema->srcTableID = srcTableID;
    schema->dstTableID = dstTableID;
    return schema;
}

std::string RelTableSchema::toCypher(ClientContext* clientContext) const {
    std::stringstream ss;
    auto catalog = clientContext->getCatalog();
    auto srcTableName = catalog->getTableName(clientContext->getTx(), srcTableID);
    auto dstTableName = catalog->getTableName(clientContext->getTx(), dstTableID);
    ss << "CREATE REL TABLE " << tableName << "( FROM " << srcTableName << " TO " << dstTableName
       << ", ";
    CatalogExportUtils::getCypher(&properties, ss);
    auto srcMultiStr = srcMultiplicity == RelMultiplicity::MANY ? "MANY" : "ONE";
    auto dstMultiStr = dstMultiplicity == RelMultiplicity::MANY ? "MANY" : "ONE";
    ss << srcMultiStr << "_" << dstMultiStr << ");";
    return ss.str();
}

} // namespace catalog
} // namespace kuzu
