#include "src/storage/include/catalog.h"

#include "spdlog/spdlog.h"

using namespace std;
using namespace graphflow::storage;

namespace graphflow {
namespace common {

/**
 * Specialized serialize and deserialize functions used in Catalog.
 * */

template<>
uint64_t SerDeser::serializeValue<string>(const string& value, int fd, uint64_t offset) {
    uint64_t valueLength = value.length();
    FileUtils::writeToFile(fd, &valueLength, sizeof(uint64_t), offset);
    FileUtils::writeToFile(fd, (uint8_t*)value.data(), valueLength, offset + sizeof(uint64_t));
    return offset + sizeof(uint64_t) + valueLength;
}

template<>
uint64_t SerDeser::deserializeValue<string>(string& value, int fd, uint64_t offset) {
    uint64_t valueLength = 0;
    offset = SerDeser::deserializeValue<uint64_t>(valueLength, fd, offset);
    value.resize(valueLength);
    FileUtils::readFromFile(fd, (uint8_t*)value.data(), valueLength, offset);
    return offset + valueLength;
}

template<>
uint64_t SerDeser::serializeValue<PropertyDefinition>(
    const PropertyDefinition& value, int fd, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.name, fd, offset);
    offset = SerDeser::serializeValue<uint32_t>(value.id, fd, offset);
    offset = SerDeser::serializeValue<DataType>(value.dataType, fd, offset);
    return SerDeser::serializeValue<bool>(value.isPrimaryKey, fd, offset);
}

template<>
uint64_t SerDeser::deserializeValue<PropertyDefinition>(
    PropertyDefinition& value, int fd, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.name, fd, offset);
    offset = SerDeser::deserializeValue<uint32_t>(value.id, fd, offset);
    offset = SerDeser::deserializeValue<DataType>(value.dataType, fd, offset);
    return SerDeser::deserializeValue<bool>(value.isPrimaryKey, fd, offset);
}

template<>
uint64_t SerDeser::serializeValue<NodeLabelDefinition>(
    const NodeLabelDefinition& value, int fd, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.labelName, fd, offset);
    offset = SerDeser::serializeValue<label_t>(value.labelId, fd, offset);
    offset = SerDeser::serializeValue<uint64_t>(value.primaryPropertyId, fd, offset);
    offset = SerDeser::serializeVector<PropertyDefinition>(value.properties, fd, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(value.fwdRelLabelIdSet, fd, offset);
    return SerDeser::serializeUnorderedSet<label_t>(value.bwdRelLabelIdSet, fd, offset);
}

template<>
uint64_t SerDeser::deserializeValue<NodeLabelDefinition>(
    NodeLabelDefinition& value, int fd, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.labelName, fd, offset);
    offset = SerDeser::deserializeValue<label_t>(value.labelId, fd, offset);
    offset = SerDeser::deserializeValue<uint64_t>(value.primaryPropertyId, fd, offset);
    offset = SerDeser::deserializeVector<PropertyDefinition>(value.properties, fd, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(value.fwdRelLabelIdSet, fd, offset);
    return SerDeser::deserializeUnorderedSet<label_t>(value.bwdRelLabelIdSet, fd, offset);
}

template<>
uint64_t SerDeser::serializeValue<RelLabelDefinition>(
    const RelLabelDefinition& value, int fd, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.labelName, fd, offset);
    offset = SerDeser::serializeValue<label_t>(value.labelId, fd, offset);
    offset = SerDeser::serializeValue<RelMultiplicity>(value.relMultiplicity, fd, offset);
    offset = SerDeser::serializeVector<PropertyDefinition>(value.properties, fd, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(value.srcNodeLabelIdSet, fd, offset);
    return SerDeser::serializeUnorderedSet<label_t>(value.dstNodeLabelIdSet, fd, offset);
}

template<>
uint64_t SerDeser::deserializeValue<RelLabelDefinition>(
    RelLabelDefinition& value, int fd, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.labelName, fd, offset);
    offset = SerDeser::deserializeValue<label_t>(value.labelId, fd, offset);
    offset = SerDeser::deserializeValue<RelMultiplicity>(value.relMultiplicity, fd, offset);
    offset = SerDeser::deserializeVector<PropertyDefinition>(value.properties, fd, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(value.srcNodeLabelIdSet, fd, offset);
    return SerDeser::deserializeUnorderedSet<label_t>(value.dstNodeLabelIdSet, fd, offset);
}

} // namespace common
} // namespace graphflow

namespace graphflow {
namespace storage {

RelMultiplicity getRelMultiplicity(const string& relMultiplicityString) {
    if ("ONE_ONE" == relMultiplicityString) {
        return ONE_ONE;
    } else if ("MANY_ONE" == relMultiplicityString) {
        return MANY_ONE;
    } else if ("ONE_MANY" == relMultiplicityString) {
        return ONE_MANY;
    } else if ("MANY_MANY" == relMultiplicityString) {
        return MANY_MANY;
    }
    throw invalid_argument("Invalid relMultiplicity string \"" + relMultiplicityString + "\"");
}

Catalog::Catalog() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    registerBuiltInFunctions();
}

Catalog::Catalog(const string& directory) : Catalog() {
    logger->info("Initializing catalog.");
    readFromFile(directory);
    registerBuiltInFunctions();
    logger->info("Initializing catalog done.");
}

void Catalog::addNodeLabel(
    string labelName, vector<PropertyDefinition> properties, const string& primaryKeyPropertyName) {
    label_t labelId = nodeLabels.size();
    bool hasFoundPrimaryKey = false;
    uint64_t primaryKeyPropertyId;
    for (auto i = 0u; i < properties.size(); i++) {
        if (properties[i].name == primaryKeyPropertyName) {
            hasFoundPrimaryKey = true;
            properties[i].isPrimaryKey = true;
            primaryKeyPropertyId = i;
            break;
        }
    }
    if (!hasFoundPrimaryKey) {
        throw invalid_argument(
            "Specified primary key property" + primaryKeyPropertyName + " is not found.");
    }
    nodeLabelNameToIdMap[labelName] = labelId;
    nodeLabels.emplace_back(move(labelName), labelId, primaryKeyPropertyId, move(properties));
}

void Catalog::addRelLabel(string labelName, RelMultiplicity relMultiplicity,
    vector<PropertyDefinition> properties, const vector<string>& srcNodeLabelNames,
    const vector<string>& dstNodeLabelNames) {
    label_t labelId = relLabels.size();
    unordered_set<label_t> srcNodeLabelIdSet, dstNodeLabelIdSet;
    for (auto& nodeLabelName : srcNodeLabelNames) {
        if (nodeLabelNameToIdMap.find(nodeLabelName) == nodeLabelNameToIdMap.end()) {
            throw invalid_argument(
                "Specified src node " + nodeLabelName + " is not found in the catalog.");
        }
        srcNodeLabelIdSet.insert(nodeLabelNameToIdMap[nodeLabelName]);
    }
    for (auto& nodeLabelName : dstNodeLabelNames) {
        if (nodeLabelNameToIdMap.find(nodeLabelName) == nodeLabelNameToIdMap.end()) {
            throw invalid_argument(
                "Specified dst node " + nodeLabelName + " is not found in the catalog.");
        }
        dstNodeLabelIdSet.insert(nodeLabelNameToIdMap[nodeLabelName]);
    }
    for (auto& nodeLabelId : srcNodeLabelIdSet) {
        nodeLabels[nodeLabelId].fwdRelLabelIdSet.insert(labelId);
    }
    for (auto& nodeLabelId : dstNodeLabelIdSet) {
        nodeLabels[nodeLabelId].bwdRelLabelIdSet.insert(labelId);
    }
    relLabelNameToIdMap[labelName] = labelId;
    relLabels.emplace_back(move(labelName), labelId, relMultiplicity, move(properties),
        move(srcNodeLabelIdSet), move(dstNodeLabelIdSet));
}

void Catalog::addNodeUnstrProperty(uint64_t labelId, const string& propertyName) {
    auto& nodeLabel = nodeLabels[labelId];
    if (nodeLabel.unstrPropertiesNameToIdMap.contains(propertyName)) {
        // Unstructured property with the same name already exists, skip it.
        return;
    }
    auto propertyId = nodeLabel.properties.size();
    nodeLabel.properties.emplace_back(propertyName, propertyId, UNSTRUCTURED);
    nodeLabel.unstrPropertiesNameToIdMap[propertyName] = propertyId;
}

bool Catalog::containNodeProperty(label_t labelId, const string& propertyName) const {
    for (auto& property : nodeLabels[labelId].properties) {
        if (propertyName == property.name) {
            return true;
        }
    }
    return false;
}

const PropertyDefinition& Catalog::getNodeProperty(
    label_t labelId, const string& propertyName) const {
    for (auto& property : nodeLabels[labelId].properties) {
        if (propertyName == property.name) {
            return property;
        }
    }
    assert(false);
}

string Catalog::getNodePropertyAsString(label_t labelId, uint32_t propertyID) const {
    for (auto& property : nodeLabels[labelId].properties) {
        if (propertyID == property.id) {
            return property.name;
        }
    }
}

bool Catalog::containRelProperty(label_t labelId, const string& propertyName) const {
    for (auto& property : relLabels[labelId].properties) {
        if (propertyName == property.name) {
            return true;
        }
    }
    return false;
}

const PropertyDefinition& Catalog::getRelProperty(
    label_t labelId, const string& propertyName) const {
    for (auto& property : relLabels[labelId].properties) {
        if (propertyName == property.name) {
            return property;
        }
    }
    assert(false);
}

const unordered_set<label_t>& Catalog::getRelLabelsForNodeLabelDirection(
    label_t nodeLabel, Direction direction) const {
    if (nodeLabel >= nodeLabels.size()) {
        throw invalid_argument("Node label " + to_string(nodeLabel) + " is out of bounds.");
    }
    if (FWD == direction) {
        return nodeLabels[nodeLabel].fwdRelLabelIdSet;
    }
    return nodeLabels[nodeLabel].bwdRelLabelIdSet;
}

const unordered_set<label_t>& Catalog::getNodeLabelsForRelLabelDirection(
    label_t relLabel, Direction direction) const {
    if (relLabel >= relLabels.size()) {
        throw invalid_argument("Rel label " + to_string(relLabel) + " is out of bounds.");
    }
    if (FWD == direction) {
        return relLabels[relLabel].srcNodeLabelIdSet;
    }
    return relLabels[relLabel].dstNodeLabelIdSet;
}

bool Catalog::isSingleMultiplicityInDirection(label_t relLabel, Direction direction) const {
    auto relMultiplicity = relLabels[relLabel].relMultiplicity;
    if (FWD == direction) {
        return ONE_ONE == relMultiplicity || MANY_ONE == relMultiplicity;
    } else {
        return ONE_ONE == relMultiplicity || ONE_MANY == relMultiplicity;
    }
}

void Catalog::saveToFile(const string& directory) {
    auto catalogPath = FileUtils::joinPath(directory, "catalog.bin");
    int fd = FileUtils::openFile(catalogPath, O_WRONLY | O_CREAT);
    uint64_t offset = 0;
    uint64_t numNodeLabels = nodeLabels.size();
    uint64_t numRelLabels = relLabels.size();
    offset = SerDeser::serializeValue<uint64_t>(numNodeLabels, fd, offset);
    offset = SerDeser::serializeValue<uint64_t>(numRelLabels, fd, offset);
    for (auto& nodeLabel : nodeLabels) {
        offset = SerDeser::serializeValue<NodeLabelDefinition>(nodeLabel, fd, offset);
    }
    for (auto& relLabel : relLabels) {
        offset = SerDeser::serializeValue<RelLabelDefinition>(relLabel, fd, offset);
    }
    FileUtils::closeFile(fd);
}

void Catalog::readFromFile(const string& directory) {
    auto catalogPath = FileUtils::joinPath(directory, "catalog.bin");
    logger->debug("Reading from {}.", catalogPath);
    int fd = FileUtils::openFile(catalogPath, O_RDONLY);
    uint64_t offset = 0;
    uint64_t numNodeLabels, numRelLabels;
    offset = SerDeser::deserializeValue<uint64_t>(numNodeLabels, fd, offset);
    offset = SerDeser::deserializeValue<uint64_t>(numRelLabels, fd, offset);
    nodeLabels.resize(numNodeLabels);
    relLabels.resize(numRelLabels);
    for (auto labelId = 0u; labelId < numNodeLabels; labelId++) {
        offset = SerDeser::deserializeValue<NodeLabelDefinition>(nodeLabels[labelId], fd, offset);
    }
    for (auto labelId = 0u; labelId < numRelLabels; labelId++) {
        offset = SerDeser::deserializeValue<RelLabelDefinition>(relLabels[labelId], fd, offset);
    }
    // construct the labelNameToIdMap and label's unstrPropertiesNameToIdMap
    for (auto& label : nodeLabels) {
        nodeLabelNameToIdMap[label.labelName] = label.labelId;
        for (auto i = 0u; i < label.properties.size(); i++) {
            auto& property = label.properties[i];
            if (property.dataType == UNSTRUCTURED) {
                label.unstrPropertiesNameToIdMap[property.name] = property.id;
            }
        }
    }
    for (auto& label : relLabels) {
        relLabelNameToIdMap[label.labelName] = label.labelId;
    }
    FileUtils::closeFile(fd);
}

static unique_ptr<nlohmann::json> getPropertiesJson(const vector<PropertyDefinition>& properties) {
    auto propertiesJson = make_unique<nlohmann::json>();
    for (const auto& property : properties) {
        nlohmann::json propertyJson =
            nlohmann::json{{"dataType", graphflow::common::DataTypeNames[property.dataType]},
                {"propIdx", to_string(property.id)},
                {"isPrimaryKey", to_string(property.isPrimaryKey)}};
        (*propertiesJson)[property.name] = propertyJson;
    }
    return propertiesJson;
}

unique_ptr<nlohmann::json> Catalog::debugInfo() {
    auto json = make_unique<nlohmann::json>();
    for (auto& label : nodeLabels) {
        string labelName = label.labelName;
        (*json)["Catalog"]["NodeLabels"][labelName]["id"] = to_string(label.labelId);
        (*json)["Catalog"]["NodeLabels"][labelName]["properties"] =
            *getPropertiesJson(label.properties);
    }

    for (auto& label : relLabels) {
        string labelName = label.labelName;
        (*json)["Catalog"]["RelLabels"][labelName]["id"] = to_string(label.labelId);
        (*json)["Catalog"]["RelLabels"][labelName]["relMultiplicity"] =
            RelMultiplicityNames[label.relMultiplicity];
        (*json)["Catalog"]["RelLabels"][labelName]["properties"] =
            *getPropertiesJson(label.properties);
        (*json)["Catalog"]["RelLabels"][labelName]["srcNodeLabels"] =
            getNodeLabelsString(label.srcNodeLabelIdSet);
        (*json)["Catalog"]["RelLabels"][labelName]["dstNodeLabels"] =
            getNodeLabelsString(label.dstNodeLabelIdSet);
    }
    return json;
}

string Catalog::getNodeLabelsString(const unordered_set<label_t>& nodeLabelIds) const {
    string nodeLabelsStr;
    bool first = true;
    for (label_t nodeLabelId : nodeLabelIds) {
        if (first) {
            first = false;
        } else {
            nodeLabelsStr += ", ";
        }
        nodeLabelsStr += nodeLabels[nodeLabelId].labelName;
    }
    return nodeLabelsStr;
}

void Catalog::registerBuiltInFunctions() {
    builtInFunctions.insert({COUNT_STAR_FUNC_NAME, make_unique<CountStarFunc>()});
    builtInFunctions.insert({ID_FUNC_NAME, make_unique<IDFunc>()});
    builtInFunctions.insert({DATE_FUNC_NAME, make_unique<DateFunc>()});
}

} // namespace storage
} // namespace graphflow
