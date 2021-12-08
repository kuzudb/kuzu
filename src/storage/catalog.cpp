#include "src/storage/include/catalog.h"

using namespace std;
using namespace graphflow::storage;

namespace graphflow {
namespace common {

/**
 * Specialized serialize and deserialize functions used in Catalog.
 * */

template<>
uint64_t SerDeser::serializeValue<string>(
    const string& value, FileInfo* fileInfo, uint64_t offset) {
    uint64_t valueLength = value.length();
    FileUtils::writeToFile(fileInfo, &valueLength, sizeof(uint64_t), offset);
    FileUtils::writeToFile(
        fileInfo, (uint8_t*)value.data(), valueLength, offset + sizeof(uint64_t));
    return offset + sizeof(uint64_t) + valueLength;
}

template<>
uint64_t SerDeser::deserializeValue<string>(string& value, FileInfo* fileInfo, uint64_t offset) {
    uint64_t valueLength = 0;
    offset = SerDeser::deserializeValue<uint64_t>(valueLength, fileInfo, offset);
    value.resize(valueLength);
    FileUtils::readFromFile(fileInfo, (uint8_t*)value.data(), valueLength, offset);
    return offset + valueLength;
}

template<>
uint64_t SerDeser::serializeValue<PropertyDefinition>(
    const PropertyDefinition& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.name, fileInfo, offset);
    offset = SerDeser::serializeValue<uint32_t>(value.id, fileInfo, offset);
    offset = SerDeser::serializeValue<DataType>(value.dataType, fileInfo, offset);
    return SerDeser::serializeValue<bool>(value.isPrimaryKey, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<PropertyDefinition>(
    PropertyDefinition& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.name, fileInfo, offset);
    offset = SerDeser::deserializeValue<uint32_t>(value.id, fileInfo, offset);
    offset = SerDeser::deserializeValue<DataType>(value.dataType, fileInfo, offset);
    return SerDeser::deserializeValue<bool>(value.isPrimaryKey, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue<NodeLabelDefinition>(
    const NodeLabelDefinition& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::serializeValue<label_t>(value.labelId, fileInfo, offset);
    offset = SerDeser::serializeValue<uint64_t>(value.primaryPropertyId, fileInfo, offset);
    offset =
        SerDeser::serializeVector<PropertyDefinition>(value.structuredProperties, fileInfo, offset);
    offset = SerDeser::serializeVector<PropertyDefinition>(
        value.unstructuredProperties, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(value.fwdRelLabelIdSet, fileInfo, offset);
    return SerDeser::serializeUnorderedSet<label_t>(value.bwdRelLabelIdSet, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<NodeLabelDefinition>(
    NodeLabelDefinition& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::deserializeValue<label_t>(value.labelId, fileInfo, offset);
    offset = SerDeser::deserializeValue<uint64_t>(value.primaryPropertyId, fileInfo, offset);
    offset = SerDeser::deserializeVector<PropertyDefinition>(
        value.structuredProperties, fileInfo, offset);
    offset = SerDeser::deserializeVector<PropertyDefinition>(
        value.unstructuredProperties, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(value.fwdRelLabelIdSet, fileInfo, offset);
    return SerDeser::deserializeUnorderedSet<label_t>(value.bwdRelLabelIdSet, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue<RelLabelDefinition>(
    const RelLabelDefinition& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::serializeValue<label_t>(value.labelId, fileInfo, offset);
    offset = SerDeser::serializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    offset = SerDeser::serializeVector<PropertyDefinition>(value.properties, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(value.srcNodeLabelIdSet, fileInfo, offset);
    return SerDeser::serializeUnorderedSet<label_t>(value.dstNodeLabelIdSet, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<RelLabelDefinition>(
    RelLabelDefinition& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::deserializeValue<label_t>(value.labelId, fileInfo, offset);
    offset = SerDeser::deserializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    offset = SerDeser::deserializeVector<PropertyDefinition>(value.properties, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(value.srcNodeLabelIdSet, fileInfo, offset);
    return SerDeser::deserializeUnorderedSet<label_t>(value.dstNodeLabelIdSet, fileInfo, offset);
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
}

Catalog::Catalog(const string& directory) : Catalog() {
    logger->info("Initializing catalog.");
    readFromFile(directory);
    logger->info("Initializing catalog done.");
}

void Catalog::addNodeLabel(string labelName, vector<PropertyDefinition> colHeaderDefinitions) {
    label_t labelId = nodeLabels.size();
    uint64_t primaryKeyPropertyId;
    for (auto i = 0u; i < colHeaderDefinitions.size(); i++) {
        colHeaderDefinitions[i].id = i;
        if (colHeaderDefinitions[i].name == LoaderConfig::ID_FIELD) {
            primaryKeyPropertyId = i;
        }
    }
    nodeLabelNameToIdMap[labelName] = labelId;
    nodeLabels.emplace_back(
        move(labelName), labelId, primaryKeyPropertyId, move(colHeaderDefinitions));
}

void Catalog::addRelLabel(string labelName, RelMultiplicity relMultiplicity,
    vector<PropertyDefinition> colHeaderDefinitions, const vector<string>& srcNodeLabelNames,
    const vector<string>& dstNodeLabelNames) {
    label_t labelId = relLabels.size();
    unordered_set<label_t> srcNodeLabelIdSet, dstNodeLabelIdSet;
    for (auto& nodeLabelName : srcNodeLabelNames) {
        if (nodeLabelNameToIdMap.find(nodeLabelName) == nodeLabelNameToIdMap.end()) {
            throw invalid_argument("Specified src node label `" + nodeLabelName +
                                   "` of rel label `" + labelName +
                                   "` is not found in the catalog.");
        }
        srcNodeLabelIdSet.insert(nodeLabelNameToIdMap[nodeLabelName]);
    }
    for (auto& nodeLabelName : dstNodeLabelNames) {
        if (nodeLabelNameToIdMap.find(nodeLabelName) == nodeLabelNameToIdMap.end()) {
            throw invalid_argument("Specified dst node node `" + nodeLabelName +
                                   "` of rel label `" + labelName +
                                   "` is not found in the catalog.");
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
    // filter out mandatory fields from column header definitions and give propertyId only to the
    // structured property definitions.
    vector<PropertyDefinition> propertyDefinitions;
    auto propertyId = 0;
    for (auto& colHeaderDefinition : colHeaderDefinitions) {
        auto name = colHeaderDefinition.name;
        if (name == LoaderConfig::START_ID_FIELD || name == LoaderConfig::END_ID_FIELD ||
            name == LoaderConfig::START_ID_LABEL_FIELD ||
            name == LoaderConfig::END_ID_LABEL_FIELD) {
            continue;
        }
        colHeaderDefinition.id = propertyId++;
        propertyDefinitions.emplace_back(colHeaderDefinition);
    }
    relLabels.emplace_back(move(labelName), labelId, relMultiplicity, move(propertyDefinitions),
        move(srcNodeLabelIdSet), move(dstNodeLabelIdSet));
}

void Catalog::addNodeUnstrProperty(uint64_t labelId, const string& propertyName) {
    auto& nodeLabel = nodeLabels[labelId];
    if (nodeLabel.unstrPropertiesNameToIdMap.contains(propertyName)) {
        // Unstructured property with the same name already exists, skip it.
        return;
    }
    auto propertyId = nodeLabel.unstructuredProperties.size();
    nodeLabel.unstructuredProperties.emplace_back(propertyName, propertyId, UNSTRUCTURED);
    nodeLabel.unstrPropertiesNameToIdMap[propertyName] = propertyId;
}

bool Catalog::containNodeProperty(label_t labelId, const string& propertyName) const {
    for (auto& property : nodeLabels[labelId].structuredProperties) {
        if (propertyName == property.name) {
            return true;
        }
    }
    return nodeLabels[labelId].unstrPropertiesNameToIdMap.contains(propertyName);
}

const PropertyDefinition& Catalog::getNodeProperty(
    label_t labelId, const string& propertyName) const {
    for (auto& property : nodeLabels[labelId].structuredProperties) {
        if (propertyName == property.name) {
            return property;
        }
    }
    auto unstrPropertyIdx = getUnstrPropertiesNameToIdMap(labelId).at(propertyName);
    return getUnstructuredNodeProperties(labelId)[unstrPropertyIdx];
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

const vector<PropertyDefinition> Catalog::getAllNodeProperties(label_t nodeLabel) const {
    auto allProperties = nodeLabels[nodeLabel].structuredProperties;
    allProperties.insert(allProperties.end(), nodeLabels[nodeLabel].unstructuredProperties.begin(),
        nodeLabels[nodeLabel].unstructuredProperties.end());
    return allProperties;
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
    auto fileInfo = FileUtils::openFile(catalogPath, O_WRONLY | O_CREAT);
    uint64_t offset = 0;
    uint64_t numNodeLabels = nodeLabels.size();
    uint64_t numRelLabels = relLabels.size();
    offset = SerDeser::serializeValue<uint64_t>(numNodeLabels, fileInfo.get(), offset);
    offset = SerDeser::serializeValue<uint64_t>(numRelLabels, fileInfo.get(), offset);
    for (auto& nodeLabel : nodeLabels) {
        offset = SerDeser::serializeValue<NodeLabelDefinition>(nodeLabel, fileInfo.get(), offset);
    }
    for (auto& relLabel : relLabels) {
        offset = SerDeser::serializeValue<RelLabelDefinition>(relLabel, fileInfo.get(), offset);
    }
    FileUtils::closeFile(fileInfo->fd);
}

void Catalog::readFromFile(const string& directory) {
    auto catalogPath = FileUtils::joinPath(directory, "catalog.bin");
    logger->debug("Reading from {}.", catalogPath);
    auto fileInfo = FileUtils::openFile(catalogPath, O_RDONLY);
    uint64_t offset = 0;
    uint64_t numNodeLabels, numRelLabels;
    offset = SerDeser::deserializeValue<uint64_t>(numNodeLabels, fileInfo.get(), offset);
    offset = SerDeser::deserializeValue<uint64_t>(numRelLabels, fileInfo.get(), offset);
    nodeLabels.resize(numNodeLabels);
    relLabels.resize(numRelLabels);
    for (auto labelId = 0u; labelId < numNodeLabels; labelId++) {
        offset = SerDeser::deserializeValue<NodeLabelDefinition>(
            nodeLabels[labelId], fileInfo.get(), offset);
    }
    for (auto labelId = 0u; labelId < numRelLabels; labelId++) {
        offset = SerDeser::deserializeValue<RelLabelDefinition>(
            relLabels[labelId], fileInfo.get(), offset);
    }
    // construct the labelNameToIdMap and label's unstrPropertiesNameToIdMap
    for (auto& label : nodeLabels) {
        nodeLabelNameToIdMap[label.labelName] = label.labelId;
        for (auto i = 0u; i < label.unstructuredProperties.size(); i++) {
            auto& property = label.unstructuredProperties[i];
            if (property.dataType == UNSTRUCTURED) {
                label.unstrPropertiesNameToIdMap[property.name] = property.id;
            }
        }
    }
    for (auto& label : relLabels) {
        relLabelNameToIdMap[label.labelName] = label.labelId;
    }
    FileUtils::closeFile(fileInfo->fd);
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
        (*json)["Catalog"]["NodeLabels"][labelName]["structured properties"] =
            *getPropertiesJson(label.structuredProperties);
        (*json)["Catalog"]["NodeLabels"][labelName]["unstructured properties"] =
            *getPropertiesJson(label.unstructuredProperties);
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

} // namespace storage
} // namespace graphflow
