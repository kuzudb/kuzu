#include "catalog/node_table_schema.h"

#include <sstream>

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

NodeTableSchema::NodeTableSchema(const NodeTableSchema& other) : TableSchema{other} {
    primaryKeyPID = other.primaryKeyPID;
    fwdRelTableIDSet = other.fwdRelTableIDSet;
    bwdRelTableIDSet = other.bwdRelTableIDSet;
}

void NodeTableSchema::serializeInternal(Serializer& serializer) {
    serializer.serializeValue(primaryKeyPID);
    serializer.serializeUnorderedSet(fwdRelTableIDSet);
    serializer.serializeUnorderedSet(bwdRelTableIDSet);
}

std::unique_ptr<NodeTableSchema> NodeTableSchema::deserialize(Deserializer& deserializer) {
    property_id_t primaryKeyPID;
    std::unordered_set<table_id_t> fwdRelTableIDSet;
    std::unordered_set<table_id_t> bwdRelTableIDSet;
    deserializer.deserializeValue(primaryKeyPID);
    deserializer.deserializeUnorderedSet(fwdRelTableIDSet);
    deserializer.deserializeUnorderedSet(bwdRelTableIDSet);
    auto schema = std::make_unique<NodeTableSchema>();
    schema->primaryKeyPID = primaryKeyPID;
    schema->fwdRelTableIDSet = std::move(fwdRelTableIDSet);
    schema->bwdRelTableIDSet = std::move(bwdRelTableIDSet);
    return schema;
}

std::string NodeTableSchema::toCypher() const {
    std::stringstream ss;
    ss << "CREATE NODE TABLE " << tableName << "(";
    for (auto& prop : properties) {
        auto propStr = prop.getDataType()->toString();
        StringUtils::replaceAll(propStr, ":", " ");
        if (propStr.find("MAP") != std::string::npos) {
            StringUtils::replaceAll(propStr, "  ", ",");
        }
        ss << prop.getName() << " " << propStr << ",";
    }
    ss << " PRIMARY KEY(" << getPrimaryKey()->getName() << "));";
    return ss.str();
}

} // namespace catalog
} // namespace kuzu
