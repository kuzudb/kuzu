#include "catalog/rdf_graph_schema.h"

#include "common/keyword/rdf_keyword.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

RdfGraphSchema::RdfGraphSchema(const RdfGraphSchema& other) : TableSchema{other} {
    resourceTableID = other.resourceTableID;
    literalTableID = other.literalTableID;
    resourceTripleTableID = other.resourceTripleTableID;
    literalTripleTableID = other.literalTripleTableID;
}

bool RdfGraphSchema::isParent(common::table_id_t tableID) {
    return tableID == resourceTableID || tableID == literalTableID ||
           tableID == resourceTripleTableID || tableID == literalTripleTableID;
}

void RdfGraphSchema::serializeInternal(Serializer& serializer) {
    serializer.serializeValue(resourceTableID);
    serializer.serializeValue(literalTableID);
    serializer.serializeValue(resourceTripleTableID);
    serializer.serializeValue(literalTripleTableID);
}

std::unique_ptr<RdfGraphSchema> RdfGraphSchema::deserialize(Deserializer& deserializer) {
    auto schema = std::make_unique<RdfGraphSchema>();
    deserializer.deserializeValue(schema->resourceTableID);
    deserializer.deserializeValue(schema->literalTableID);
    deserializer.deserializeValue(schema->resourceTripleTableID);
    deserializer.deserializeValue(schema->literalTripleTableID);
    return schema;
}

std::string RdfGraphSchema::getResourceTableName(const std::string& graphName) {
    return graphName + std::string(rdf::RESOURCE_TABLE_SUFFIX);
}

std::string RdfGraphSchema::getLiteralTableName(const std::string& graphName) {
    return graphName + std::string(rdf::LITERAL_TABLE_SUFFIX);
}

std::string RdfGraphSchema::getResourceTripleTableName(const std::string& graphName) {
    return graphName + std::string(rdf::RESOURCE_TRIPLE_TABLE_SUFFIX);
}

std::string RdfGraphSchema::getLiteralTripleTableName(const std::string& graphName) {
    return graphName + std::string(rdf::LITERAL_TRIPLE_TABLE_SUFFIX);
}

} // namespace catalog
} // namespace kuzu
