#include "catalog/property_definition_collection.h"

#include <sstream>

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_utils.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace catalog {

const PropertyDefinition& PropertyDefinitionCollection::getDefinition(
    const std::string& name) const {
    return getDefinition(getIdx(name));
}

const PropertyDefinition& PropertyDefinitionCollection::getDefinition(idx_t idx) const {
    KU_ASSERT(idx < definitions.size());
    return definitions[idx];
}

column_id_t PropertyDefinitionCollection::getColumnID(const std::string& name) const {
    return getColumnID(getIdx(name));
}

column_id_t PropertyDefinitionCollection::getColumnID(idx_t idx) const {
    KU_ASSERT(idx < columnIDs.size());
    return columnIDs[idx];
}

void PropertyDefinitionCollection::vacuumColumnIDs(column_id_t nextColumnID) {
    this->nextColumnID = nextColumnID;
    columnIDs.clear();
    for (auto& _ : definitions) {
        KU_UNUSED(_);
        columnIDs.push_back(this->nextColumnID++);
    }
}

void PropertyDefinitionCollection::add(const PropertyDefinition& definition) {
    nameToPropertyIdxMap.insert({definition.columnDefinition.name, definitions.size()});
    columnIDs.push_back(nextColumnID++);
    definitions.push_back(definition.copy());
}

void PropertyDefinitionCollection::drop(const std::string& name) {
    KU_ASSERT(contains(name));
    auto idx = nameToPropertyIdxMap.at(name);
    definitions.erase(definitions.begin() + idx);
    columnIDs.erase(columnIDs.begin() + idx);
    nameToPropertyIdxMap.clear();
    for (auto i = 0u; i < definitions.size(); ++i) {
        nameToPropertyIdxMap.insert({definitions[i].getName(), i});
    }
}

void PropertyDefinitionCollection::rename(const std::string& name, const std::string& newName) {
    KU_ASSERT(contains(name));
    auto idx = nameToPropertyIdxMap.at(name);
    definitions[idx].rename(newName);
    nameToPropertyIdxMap.erase(name);
    nameToPropertyIdxMap.insert({newName, idx});
}

column_id_t PropertyDefinitionCollection::getMaxColumnID() const {
    column_id_t maxID = 0;
    for (auto id : columnIDs) {
        if (id > maxID) {
            maxID = id;
        }
    }
    return maxID;
}

idx_t PropertyDefinitionCollection::getIdx(const std::string& name) const {
    KU_ASSERT(contains(name));
    return nameToPropertyIdxMap.at(name);
}

std::string PropertyDefinitionCollection::toCypher() const {
    std::stringstream ss;
    for (auto& def : definitions) {
        auto& dataType = def.getType();
        if (dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID) {
            continue;
        }
        auto typeStr = dataType.toString();
        StringUtils::replaceAll(typeStr, ":", " ");
        if (typeStr.find("MAP") != std::string::npos) {
            StringUtils::replaceAll(typeStr, "  ", ",");
        }
        ss << def.getName() << " " << typeStr << ",";
    }
    return ss.str();
}

void PropertyDefinitionCollection::serialize(Serializer& serializer) const {
    serializer.writeDebuggingInfo("nextColumnID");
    serializer.serializeValue(nextColumnID);
    serializer.writeDebuggingInfo("definitions");
    serializer.serializeVector(definitions);
    serializer.writeDebuggingInfo("columnIDs");
    serializer.serializeVector(columnIDs);
}

PropertyDefinitionCollection PropertyDefinitionCollection::deserialize(Deserializer& deserializer) {
    std::string debuggingInfo;
    column_id_t nextColumnID = 0;
    deserializer.validateDebuggingInfo(debuggingInfo, "nextColumnID");
    deserializer.deserializeValue(nextColumnID);
    std::vector<PropertyDefinition> definitions;
    deserializer.validateDebuggingInfo(debuggingInfo, "definitions");
    deserializer.deserializeVector(definitions);
    std::vector<column_id_t> columnIDs;
    deserializer.validateDebuggingInfo(debuggingInfo, "columnIDs");
    deserializer.deserializeVector(columnIDs);
    auto collection = PropertyDefinitionCollection();
    for (auto i = 0u; i < definitions.size(); ++i) {
        collection.nameToPropertyIdxMap.insert({definitions[i].getName(), i});
    }
    collection.nextColumnID = nextColumnID;
    collection.definitions = std::move(definitions);
    collection.columnIDs = std::move(columnIDs);
    return collection;
}

} // namespace catalog
} // namespace kuzu
