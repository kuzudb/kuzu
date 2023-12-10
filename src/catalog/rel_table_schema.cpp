#include "catalog/rel_table_schema.h"

#include "common/exception/binder.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

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

} // namespace catalog
} // namespace kuzu
