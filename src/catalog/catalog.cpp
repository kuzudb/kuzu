#include "include/catalog.h"

using namespace std;
using namespace graphflow::catalog;

namespace graphflow {
namespace common {

/**
 * Specialized serialize and deserialize functions used in Catalog.
 * */

template<>
uint64_t SerDeser::serializeValue<string>(
    const string& value, FileInfo* fileInfo, uint64_t offset) {
    uint64_t valueLength = value.length();
    FileUtils::writeToFile(fileInfo, (uint8_t*)&valueLength, sizeof(uint64_t), offset);
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
uint64_t SerDeser::serializeValue<DataType>(
    const DataType& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<DataTypeID>(value.typeID, fileInfo, offset);
    if (value.childType) {
        assert(value.typeID == LIST);
        return SerDeser::serializeValue<DataType>(*value.childType, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeValue<DataType>(
    DataType& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<DataTypeID>(value.typeID, fileInfo, offset);
    if (value.typeID == LIST) {
        auto childDataType = make_unique<DataType>();
        offset = SerDeser::deserializeValue<DataType>(*childDataType, fileInfo, offset);
        value.childType = move(childDataType);
        return offset;
    }
    return offset;
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
uint64_t SerDeser::serializeValue(
    const unordered_map<label_t, uint64_t>& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<uint64_t>(value.size(), fileInfo, offset);
    for (auto& entry : value) {
        offset = SerDeser::serializeValue<label_t>(entry.first, fileInfo, offset);
        offset = SerDeser::serializeValue<uint64_t>(entry.second, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeValue(
    unordered_map<label_t, uint64_t>& value, FileInfo* fileInfo, uint64_t offset) {
    uint64_t numEntries = 0;
    offset = SerDeser::deserializeValue<uint64_t>(numEntries, fileInfo, offset);
    for (auto i = 0u; i < numEntries; i++) {
        label_t label;
        uint64_t num;
        offset = SerDeser::deserializeValue<label_t>(label, fileInfo, offset);
        offset = SerDeser::deserializeValue<uint64_t>(num, fileInfo, offset);
        value.emplace(label, num);
    }
    return offset;
}

template<>
uint64_t SerDeser::serializeVector<vector<uint64_t>>(
    const vector<vector<uint64_t>>& values, FileInfo* fileInfo, uint64_t offset) {
    uint64_t vectorSize = values.size();
    offset = SerDeser::serializeValue<uint64_t>(vectorSize, fileInfo, offset);
    for (auto& value : values) {
        offset = serializeVector<uint64_t>(value, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeVector<vector<uint64_t>>(
    vector<vector<uint64_t>>& values, FileInfo* fileInfo, uint64_t offset) {
    uint64_t vectorSize;
    offset = deserializeValue<uint64_t>(vectorSize, fileInfo, offset);
    values.resize(vectorSize);
    for (auto& value : values) {
        offset = SerDeser::deserializeVector<uint64_t>(value, fileInfo, offset);
    }
    return offset;
}

template<>
uint64_t SerDeser::serializeValue<NodeLabel>(
    const NodeLabel& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::serializeValue<label_t>(value.labelId, fileInfo, offset);
    offset = SerDeser::serializeValue<uint64_t>(value.primaryPropertyId, fileInfo, offset);
    offset =
        SerDeser::serializeVector<PropertyDefinition>(value.structuredProperties, fileInfo, offset);
    offset = SerDeser::serializeVector<PropertyDefinition>(
        value.unstructuredProperties, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(value.fwdRelLabelIdSet, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(value.bwdRelLabelIdSet, fileInfo, offset);
    return SerDeser::serializeValue<uint64_t>(value.numNodes, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<NodeLabel>(
    NodeLabel& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::deserializeValue<label_t>(value.labelId, fileInfo, offset);
    offset = SerDeser::deserializeValue<uint64_t>(value.primaryPropertyId, fileInfo, offset);
    offset = SerDeser::deserializeVector<PropertyDefinition>(
        value.structuredProperties, fileInfo, offset);
    offset = SerDeser::deserializeVector<PropertyDefinition>(
        value.unstructuredProperties, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(value.fwdRelLabelIdSet, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(value.bwdRelLabelIdSet, fileInfo, offset);
    return SerDeser::deserializeValue<uint64_t>(value.numNodes, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue<RelLabel>(
    const RelLabel& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::serializeValue<label_t>(value.labelId, fileInfo, offset);
    offset = SerDeser::serializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    offset = SerDeser::serializeVector<PropertyDefinition>(value.properties, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(value.srcNodeLabelIdSet, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(value.dstNodeLabelIdSet, fileInfo, offset);
    return SerDeser::serializeVector<unordered_map<label_t, uint64_t>>(
        value.numRelsPerDirectionBoundLabel, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<RelLabel>(
    RelLabel& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::deserializeValue<label_t>(value.labelId, fileInfo, offset);
    offset = SerDeser::deserializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    offset = SerDeser::deserializeVector<PropertyDefinition>(value.properties, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(value.srcNodeLabelIdSet, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(value.dstNodeLabelIdSet, fileInfo, offset);
    return SerDeser::deserializeVector<unordered_map<label_t, uint64_t>>(
        value.numRelsPerDirectionBoundLabel, fileInfo, offset);
}

} // namespace common
} // namespace graphflow

namespace graphflow {
namespace catalog {

RelMultiplicity getRelMultiplicityFromString(const string& relMultiplicityString) {
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

NodeLabel::NodeLabel(string labelName, label_t labelId, uint64_t primaryPropertyId,
    vector<PropertyDefinition> structuredProperties,
    const vector<string>& unstructuredPropertyNames, uint64_t numNodes)
    : Label{move(labelName), labelId}, primaryPropertyId{primaryPropertyId},
      structuredProperties{move(structuredProperties)}, numNodes{numNodes} {
    for (auto& unstrPropertyName : unstructuredPropertyNames) {
        auto unstrPropertyId = unstructuredProperties.size();
        unstrPropertiesNameToIdMap[unstrPropertyName] = unstrPropertyId;
        unstructuredProperties.emplace_back(
            unstrPropertyName, unstrPropertyId, DataType(UNSTRUCTURED));
    }
}

Catalog::Catalog() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    builtInVectorOperations = make_unique<BuiltInVectorOperations>();
    builtInAggregateFunctions = make_unique<BuiltInAggregateFunctions>();
}

Catalog::Catalog(const string& directory) : Catalog() {
    logger->info("Initializing catalog.");
    readFromFile(directory);
    logger->info("Initializing catalog done.");
}

void Catalog::verifyColDefinitionsForNodeLabel(
    uint64_t primaryKeyPropertyId, const vector<PropertyDefinition>& colHeaderDefinitions) {
    if (primaryKeyPropertyId == UINT64_MAX) {
        throw CatalogException(
            "Column header definitions of a node file does not contain the mandatory field '" +
            string(LoaderConfig::ID_FIELD) + "'.");
    }
    for (auto& colHeaderDefinition : colHeaderDefinitions) {
        auto name = colHeaderDefinition.name;
        if (name == LoaderConfig::START_ID_FIELD || name == LoaderConfig::START_ID_LABEL_FIELD ||
            name == LoaderConfig::START_ID_FIELD || name == LoaderConfig::END_ID_LABEL_FIELD) {
            throw CatalogException(
                "Column header contains a mandatory field '" + name + "' that is not allowed.");
        }
        if (colHeaderDefinition.dataType.typeID == INVALID) {
            throw CatalogException("Column header contains an INVALID data type.");
        }
        if (colHeaderDefinition.isPrimaryKey) {
            if (colHeaderDefinition.id != primaryKeyPropertyId) {
                throw CatalogException("Unexpected primary key property. Check if there are "
                                       "duplicated primary key definitions.");
            }
            if (colHeaderDefinition.dataType.typeID != STRING &&
                colHeaderDefinition.dataType.typeID != INT64) {
                throw CatalogException("Unsupported data type '" +
                                       Types::dataTypeToString(colHeaderDefinition.dataType) +
                                       "' for primary key property.");
            }
        }
    }
}

void Catalog::addNodeLabel(string labelName, const DataType& IDType,
    vector<PropertyDefinition> colHeaderDefinitions,
    const vector<string>& unstructuredPropertyNames, uint64_t numNodes) {
    label_t labelId = nodeLabels.size();
    uint64_t primaryKeyPropertyId = UINT64_MAX;
    for (auto i = 0u; i < colHeaderDefinitions.size(); i++) {
        colHeaderDefinitions[i].id = i;
        if (colHeaderDefinitions[i].isPrimaryKey) {
            colHeaderDefinitions[i].dataType = IDType;
            primaryKeyPropertyId = i;
        }
    }
    verifyColDefinitionsForNodeLabel(primaryKeyPropertyId, colHeaderDefinitions);
    nodeLabelNameToIdMap[labelName] = labelId;
    nodeLabels.emplace_back(move(labelName), labelId, primaryKeyPropertyId,
        move(colHeaderDefinitions), unstructuredPropertyNames, numNodes);
}

void Catalog::verifyColDefinitionsForRelLabel(
    const vector<PropertyDefinition>& colHeaderDefinitions) {
    auto numMandatoryFields = 0;
    for (auto& colHeaderDefinition : colHeaderDefinitions) {
        auto name = colHeaderDefinition.name;
        if (name == LoaderConfig::START_ID_FIELD || name == LoaderConfig::START_ID_LABEL_FIELD ||
            name == LoaderConfig::END_ID_FIELD || name == LoaderConfig::END_ID_LABEL_FIELD) {
            numMandatoryFields++;
        }
        if (name == LoaderConfig::ID_FIELD) {
            throw CatalogException("Column header definitions of a rel file cannot contain "
                                   "the mandatory field 'ID'.");
        }
        if (colHeaderDefinition.dataType.typeID == INVALID) {
            throw CatalogException("Column header contains an INVALID data type.");
        }
    }
    if (numMandatoryFields != 4) {
        throw CatalogException("Column header definitions of a rel file does not contains all "
                               "the mandatory field.");
    }
}

void Catalog::addRelLabel(string labelName, RelMultiplicity relMultiplicity,
    vector<PropertyDefinition> colHeaderDefinitions, const vector<string>& srcNodeLabelNames,
    const vector<string>& dstNodeLabelNames) {
    verifyColDefinitionsForRelLabel(colHeaderDefinitions);
    label_t labelId = relLabels.size();
    unordered_set<label_t> srcNodeLabelIdSet, dstNodeLabelIdSet;
    for (auto& nodeLabelName : srcNodeLabelNames) {
        if (nodeLabelNameToIdMap.find(nodeLabelName) == nodeLabelNameToIdMap.end()) {
            throw CatalogException(StringUtils::string_format(
                "Specified src node label '%s' of rel label '%s' is not found in the catalog.",
                nodeLabelName.c_str(), labelName.c_str()));
        }
        srcNodeLabelIdSet.insert(nodeLabelNameToIdMap[nodeLabelName]);
    }
    for (auto& nodeLabelName : dstNodeLabelNames) {
        if (nodeLabelNameToIdMap.find(nodeLabelName) == nodeLabelNameToIdMap.end()) {
            throw CatalogException(StringUtils::string_format(
                "Specified dst node node '%s' of rel label '%s' is not found in the catalog.",
                nodeLabelName.c_str(), labelName.c_str()));
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
        propertyDefinitions.push_back(colHeaderDefinition);
    }
    relLabels.emplace_back(move(labelName), labelId, relMultiplicity, move(propertyDefinitions),
        move(srcNodeLabelIdSet), move(dstNodeLabelIdSet));
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

vector<PropertyDefinition> Catalog::getAllNodeProperties(label_t nodeLabel) const {
    auto allProperties = nodeLabels[nodeLabel].structuredProperties;
    allProperties.insert(allProperties.end(), nodeLabels[nodeLabel].unstructuredProperties.begin(),
        nodeLabels[nodeLabel].unstructuredProperties.end());
    return allProperties;
}

const unordered_set<label_t>& Catalog::getRelLabelsForNodeLabelDirection(
    label_t nodeLabel, RelDirection direction) const {
    if (nodeLabel >= nodeLabels.size()) {
        throw CatalogException("Node label " + to_string(nodeLabel) + " is out of bounds.");
    }
    if (FWD == direction) {
        return nodeLabels[nodeLabel].fwdRelLabelIdSet;
    }
    return nodeLabels[nodeLabel].bwdRelLabelIdSet;
}

const unordered_set<label_t>& Catalog::getNodeLabelsForRelLabelDirection(
    label_t relLabel, RelDirection direction) const {
    if (relLabel >= relLabels.size()) {
        throw CatalogException("Rel label " + to_string(relLabel) + " is out of bounds.");
    }
    if (FWD == direction) {
        return relLabels[relLabel].srcNodeLabelIdSet;
    }
    return relLabels[relLabel].dstNodeLabelIdSet;
}

bool Catalog::isSingleMultiplicityInDirection(label_t relLabel, RelDirection direction) const {
    auto relMultiplicity = relLabels[relLabel].relMultiplicity;
    if (FWD == direction) {
        return ONE_ONE == relMultiplicity || MANY_ONE == relMultiplicity;
    } else {
        return ONE_ONE == relMultiplicity || ONE_MANY == relMultiplicity;
    }
}

vector<uint64_t> Catalog::getNumNodesPerLabel() const {
    vector<uint64_t> result(nodeLabels.size());
    for (auto i = 0u; i < nodeLabels.size(); i++) {
        result[i] = nodeLabels[i].numNodes;
    }
    return result;
}

uint64_t Catalog::getNumNodes(label_t label) const {
    if (label >= nodeLabels.size()) {
        throw CatalogException("Node label " + to_string(label) + " is out of bounds.");
    }
    return nodeLabels[label].numNodes;
}

uint64_t Catalog::getNumRelsForDirectionBoundLabel(
    label_t relLabel, RelDirection relDirection, label_t boundNodeLabel) const {
    if (relLabel >= relLabels.size()) {
        throw CatalogException("Rel label " + to_string(relLabel) + " is out of bounds.");
    }
    return relLabels[relLabel].numRelsPerDirectionBoundLabel[FWD == relDirection ? 0 : 1].at(
        boundNodeLabel);
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
        offset = SerDeser::serializeValue<NodeLabel>(nodeLabel, fileInfo.get(), offset);
    }
    for (auto& relLabel : relLabels) {
        offset = SerDeser::serializeValue<RelLabel>(relLabel, fileInfo.get(), offset);
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
        offset = SerDeser::deserializeValue<NodeLabel>(nodeLabels[labelId], fileInfo.get(), offset);
    }
    for (auto labelId = 0u; labelId < numRelLabels; labelId++) {
        offset = SerDeser::deserializeValue<RelLabel>(relLabels[labelId], fileInfo.get(), offset);
    }
    // construct the labelNameToIdMap and label's unstrPropertiesNameToIdMap
    for (auto& label : nodeLabels) {
        nodeLabelNameToIdMap[label.labelName] = label.labelId;
        for (auto i = 0u; i < label.unstructuredProperties.size(); i++) {
            auto& property = label.unstructuredProperties[i];
            if (property.dataType.typeID == UNSTRUCTURED) {
                label.unstrPropertiesNameToIdMap[property.name] = property.id;
            }
        }
    }
    for (auto& label : relLabels) {
        relLabelNameToIdMap[label.labelName] = label.labelId;
    }
    FileUtils::closeFile(fileInfo->fd);
}

} // namespace catalog
} // namespace graphflow
