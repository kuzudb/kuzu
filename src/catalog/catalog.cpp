#include "include/catalog.h"

#include "spdlog/spdlog.h"

#include "src/storage/include/storage_utils.h"

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
uint64_t SerDeser::serializeValue<Property>(
    const Property& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.name, fileInfo, offset);
    offset = SerDeser::serializeValue<DataType>(value.dataType, fileInfo, offset);
    offset = SerDeser::serializeValue<uint32_t>(value.propertyID, fileInfo, offset);
    offset = SerDeser::serializeValue<label_t>(value.label, fileInfo, offset);
    offset = SerDeser::serializeValue<bool>(value.isNodeLabel, fileInfo, offset);
    return SerDeser::serializeValue<bool>(value.isIDProperty(), fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<Property>(
    Property& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.name, fileInfo, offset);
    offset = SerDeser::deserializeValue<DataType>(value.dataType, fileInfo, offset);
    offset = SerDeser::deserializeValue<uint32_t>(value.propertyID, fileInfo, offset);
    offset = SerDeser::deserializeValue<label_t>(value.label, fileInfo, offset);
    offset = SerDeser::deserializeValue<bool>(value.isNodeLabel, fileInfo, offset);
    bool isIDProperty = value.isIDProperty();
    return SerDeser::deserializeValue<bool>(isIDProperty, fileInfo, offset);
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
    offset = SerDeser::serializeValue<label_t>(value.labelID, fileInfo, offset);
    offset = SerDeser::serializeValue<uint64_t>(value.primaryPropertyId, fileInfo, offset);
    offset = SerDeser::serializeVector<Property>(value.structuredProperties, fileInfo, offset);
    offset = SerDeser::serializeVector<Property>(value.unstructuredProperties, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(value.fwdRelLabelIdSet, fileInfo, offset);
    return SerDeser::serializeUnorderedSet<label_t>(value.bwdRelLabelIdSet, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<NodeLabel>(
    NodeLabel& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::deserializeValue<label_t>(value.labelID, fileInfo, offset);
    offset = SerDeser::deserializeValue<uint64_t>(value.primaryPropertyId, fileInfo, offset);
    offset = SerDeser::deserializeVector<Property>(value.structuredProperties, fileInfo, offset);
    offset = SerDeser::deserializeVector<Property>(value.unstructuredProperties, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(value.fwdRelLabelIdSet, fileInfo, offset);
    return SerDeser::deserializeUnorderedSet<label_t>(value.bwdRelLabelIdSet, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue<RelLabel>(
    const RelLabel& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::serializeValue<label_t>(value.labelID, fileInfo, offset);
    offset = SerDeser::serializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    offset = SerDeser::serializeVector<Property>(value.properties, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(
        value.srcDstLabels.srcNodeLabels, fileInfo, offset);
    offset = SerDeser::serializeUnorderedSet<label_t>(
        value.srcDstLabels.dstNodeLabels, fileInfo, offset);
    return SerDeser::serializeVector<unordered_map<label_t, uint64_t>>(
        value.numRelsPerDirectionBoundLabel, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue<RelLabel>(
    RelLabel& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue<string>(value.labelName, fileInfo, offset);
    offset = SerDeser::deserializeValue<label_t>(value.labelID, fileInfo, offset);
    offset = SerDeser::deserializeValue<RelMultiplicity>(value.relMultiplicity, fileInfo, offset);
    offset = SerDeser::deserializeVector<Property>(value.properties, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(
        value.srcDstLabels.srcNodeLabels, fileInfo, offset);
    offset = SerDeser::deserializeUnorderedSet<label_t>(
        value.srcDstLabels.dstNodeLabels, fileInfo, offset);
    return SerDeser::deserializeVector<unordered_map<label_t, uint64_t>>(
        value.numRelsPerDirectionBoundLabel, fileInfo, offset);
}

} // namespace common
} // namespace graphflow

namespace graphflow {
namespace catalog {

CatalogContent::CatalogContent() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
}

CatalogContent::CatalogContent(const string& directory) {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    logger->info("Initializing catalog.");
    readFromFile(directory, DBFileType::ORIGINAL);
    logger->info("Initializing catalog done.");
}

CatalogContent::CatalogContent(const CatalogContent& other) {
    for (auto& nodeLabel : other.nodeLabels) {
        nodeLabels.push_back(make_unique<NodeLabel>(*nodeLabel));
    }
    for (auto& relLabel : other.relLabels) {
        relLabels.push_back(make_unique<RelLabel>(*relLabel));
    }
    nodeLabelNameToIdMap = other.nodeLabelNameToIdMap;
    relLabelNameToIdMap = other.relLabelNameToIdMap;
}

label_t CatalogContent::addNodeLabel(string labelName, uint32_t primaryKeyIdx,
    vector<PropertyNameDataType> structuredPropertyDefinitions) {
    label_t labelId = getNumNodeLabels();
    vector<Property> structuredProperties;
    for (auto i = 0u; i < structuredPropertyDefinitions.size(); ++i) {
        auto& propertyDefinition = structuredPropertyDefinitions[i];
        structuredProperties.push_back(
            Property::constructStructuredNodeProperty(propertyDefinition, i, labelId));
    }
    auto nodeLabel =
        make_unique<NodeLabel>(move(labelName), labelId, primaryKeyIdx, move(structuredProperties));
    nodeLabelNameToIdMap[nodeLabel->labelName] = labelId;
    nodeLabels.push_back(move(nodeLabel));
    return labelId;
}

label_t CatalogContent::addRelLabel(string labelName, RelMultiplicity relMultiplicity,
    vector<PropertyNameDataType> structuredPropertyDefinitions, SrcDstLabels srcDstLabels) {
    label_t labelId = relLabels.size();
    for (auto& nodeLabelId : srcDstLabels.srcNodeLabels) {
        nodeLabels[nodeLabelId]->addFwdRelLabel(labelId);
    }
    for (auto& nodeLabelId : srcDstLabels.dstNodeLabels) {
        nodeLabels[nodeLabelId]->addBwdRelLabel(labelId);
    }
    vector<Property> structuredProperties;
    auto propertyId = 0;
    for (auto& propertyDefinition : structuredPropertyDefinitions) {
        structuredProperties.push_back(
            Property::constructRelProperty(propertyDefinition, propertyId++, labelId));
    }
    auto propertyNameDataType = PropertyNameDataType(INTERNAL_ID_SUFFIX, INT64);
    structuredProperties.push_back(
        Property::constructRelProperty(propertyNameDataType, propertyId++, labelId));
    auto relLabel = make_unique<RelLabel>(
        move(labelName), labelId, relMultiplicity, move(structuredProperties), move(srcDstLabels));
    relLabelNameToIdMap[relLabel->labelName] = labelId;
    relLabels.push_back(move(relLabel));
    return labelId;
}

bool CatalogContent::containNodeProperty(label_t labelId, const string& propertyName) const {
    for (auto& property : nodeLabels[labelId]->structuredProperties) {
        if (propertyName == property.name) {
            return true;
        }
    }
    return nodeLabels[labelId]->unstrPropertiesNameToIdMap.contains(propertyName);
}

bool CatalogContent::containRelProperty(label_t labelId, const string& propertyName) const {
    for (auto& property : relLabels[labelId]->properties) {
        if (propertyName == property.name) {
            return true;
        }
    }
    return false;
}

const Property& CatalogContent::getNodeProperty(label_t labelId, const string& propertyName) const {
    for (auto& property : nodeLabels[labelId]->structuredProperties) {
        if (propertyName == property.name) {
            return property;
        }
    }
    auto unstrPropertyIdx = getUnstrPropertiesNameToIdMap(labelId).at(propertyName);
    return getUnstructuredNodeProperties(labelId)[unstrPropertyIdx];
}

const Property& CatalogContent::getRelProperty(label_t labelId, const string& propertyName) const {
    for (auto& property : relLabels[labelId]->properties) {
        if (propertyName == property.name) {
            return property;
        }
    }
    assert(false);
}

const Property& CatalogContent::getNodePrimaryKeyProperty(label_t nodeLabel) const {
    auto primaryKeyId = nodeLabels[nodeLabel]->primaryPropertyId;
    return nodeLabels[nodeLabel]->structuredProperties[primaryKeyId];
}

vector<Property> CatalogContent::getAllNodeProperties(label_t nodeLabel) const {
    return nodeLabels[nodeLabel]->getAllNodeProperties();
}

const unordered_set<label_t>& CatalogContent::getRelLabelsForNodeLabelDirection(
    label_t nodeLabel, RelDirection direction) const {
    if (nodeLabel >= nodeLabels.size()) {
        throw CatalogException("Node label " + to_string(nodeLabel) + " is out of bounds.");
    }
    if (FWD == direction) {
        return nodeLabels[nodeLabel]->fwdRelLabelIdSet;
    }
    return nodeLabels[nodeLabel]->bwdRelLabelIdSet;
}

const unordered_set<label_t>& CatalogContent::getNodeLabelsForRelLabelDirection(
    label_t relLabel, RelDirection direction) const {
    if (relLabel >= relLabels.size()) {
        throw CatalogException("Rel label " + to_string(relLabel) + " is out of bounds.");
    }
    if (FWD == direction) {
        return relLabels[relLabel]->srcDstLabels.srcNodeLabels;
    }
    return relLabels[relLabel]->srcDstLabels.dstNodeLabels;
}

bool CatalogContent::isSingleMultiplicityInDirection(
    label_t relLabel, RelDirection direction) const {
    auto relMultiplicity = relLabels[relLabel]->relMultiplicity;
    if (FWD == direction) {
        return ONE_ONE == relMultiplicity || MANY_ONE == relMultiplicity;
    } else {
        return ONE_ONE == relMultiplicity || ONE_MANY == relMultiplicity;
    }
}

uint64_t CatalogContent::getNumRelsForDirectionBoundLabel(
    label_t relLabel, RelDirection relDirection, label_t boundNodeLabel) const {
    if (relLabel >= relLabels.size()) {
        throw CatalogException("Rel label " + to_string(relLabel) + " is out of bounds.");
    }
    return relLabels[relLabel]->numRelsPerDirectionBoundLabel[FWD == relDirection ? 0 : 1].at(
        boundNodeLabel);
}

void CatalogContent::saveToFile(const string& directory, DBFileType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    auto fileInfo = FileUtils::openFile(catalogPath, O_WRONLY | O_CREAT);
    uint64_t offset = 0;
    offset = SerDeser::serializeValue<uint64_t>(nodeLabels.size(), fileInfo.get(), offset);
    offset = SerDeser::serializeValue<uint64_t>(relLabels.size(), fileInfo.get(), offset);
    for (auto& nodeLabel : nodeLabels) {
        offset = SerDeser::serializeValue<NodeLabel>(*nodeLabel, fileInfo.get(), offset);
    }
    for (auto& relLabel : relLabels) {
        offset = SerDeser::serializeValue<RelLabel>(*relLabel, fileInfo.get(), offset);
    }
    FileUtils::closeFile(fileInfo->fd);
}

void CatalogContent::readFromFile(const string& directory, DBFileType dbFileType) {
    auto catalogPath = StorageUtils::getCatalogFilePath(directory, dbFileType);
    logger->debug("Reading from {}.", catalogPath);
    auto fileInfo = FileUtils::openFile(catalogPath, O_RDONLY);
    uint64_t offset = 0;
    uint64_t numNodeLabels, numRelLabels;
    offset = SerDeser::deserializeValue<uint64_t>(numNodeLabels, fileInfo.get(), offset);
    offset = SerDeser::deserializeValue<uint64_t>(numRelLabels, fileInfo.get(), offset);
    nodeLabels.resize(numNodeLabels);
    relLabels.resize(numRelLabels);
    for (auto labelId = 0u; labelId < numNodeLabels; labelId++) {
        nodeLabels[labelId] = make_unique<NodeLabel>();
        offset =
            SerDeser::deserializeValue<NodeLabel>(*nodeLabels[labelId], fileInfo.get(), offset);
    }
    for (auto labelId = 0u; labelId < numRelLabels; labelId++) {
        relLabels[labelId] = make_unique<RelLabel>();
        offset = SerDeser::deserializeValue<RelLabel>(*relLabels[labelId], fileInfo.get(), offset);
    }
    // construct the labelNameToIdMap and label's unstrPropertiesNameToIdMap
    for (auto& label : nodeLabels) {
        nodeLabelNameToIdMap[label->labelName] = label->labelID;
        for (auto i = 0u; i < label->unstructuredProperties.size(); i++) {
            auto& property = label->unstructuredProperties[i];
            if (property.dataType.typeID == UNSTRUCTURED) {
                label->unstrPropertiesNameToIdMap[property.name] = property.propertyID;
            }
        }
    }
    for (auto& label : relLabels) {
        relLabelNameToIdMap[label->labelName] = label->labelID;
    }
    FileUtils::closeFile(fileInfo->fd);
}

uint64_t CatalogContent::getNextRelID() const {
    auto nextRelID = 0ull;
    for (auto& relLabel : relLabels) {
        nextRelID += relLabel->getNumRels();
    }
    return nextRelID;
}

Catalog::Catalog() {
    catalogContentForReadOnlyTrx = make_unique<CatalogContent>();
    builtInVectorOperations = make_unique<BuiltInVectorOperations>();
    builtInAggregateFunctions = make_unique<BuiltInAggregateFunctions>();
}

Catalog::Catalog(WAL* wal) : wal{wal} {
    catalogContentForReadOnlyTrx = make_unique<CatalogContent>(wal->getDirectory());
    builtInVectorOperations = make_unique<BuiltInVectorOperations>();
    builtInAggregateFunctions = make_unique<BuiltInAggregateFunctions>();
}

void Catalog::checkpointInMemoryIfNecessary() {
    if (!hasUpdates()) {
        return;
    }
    catalogContentForReadOnlyTrx = move(catalogContentForWriteTrx);
}

ExpressionType Catalog::getFunctionType(const string& name) const {
    if (builtInVectorOperations->containsFunction(name)) {
        return FUNCTION;
    } else if (builtInAggregateFunctions->containsFunction(name)) {
        return AGGREGATE_FUNCTION;
    } else {
        throw CatalogException(name + " function does not exist.");
    }
}

label_t Catalog::addNodeLabel(string labelName, uint32_t primaryKeyIdx,
    vector<PropertyNameDataType> structuredPropertyDefinitions) {
    initCatalogContentForWriteTrxIfNecessary();
    auto labelID = catalogContentForWriteTrx->addNodeLabel(
        move(labelName), primaryKeyIdx, move(structuredPropertyDefinitions));
    wal->logNodeTableRecord(labelID);
    return labelID;
}

label_t Catalog::addRelLabel(string labelName, RelMultiplicity relMultiplicity,
    vector<PropertyNameDataType> structuredPropertyDefinitions, SrcDstLabels srcDstLabels) {
    initCatalogContentForWriteTrxIfNecessary();
    auto labelID = catalogContentForWriteTrx->addRelLabel(
        move(labelName), relMultiplicity, move(structuredPropertyDefinitions), move(srcDstLabels));
    wal->logRelTableRecord(labelID);
    return labelID;
}

void Catalog::setNumRelsPerDirectionBoundLabelOfRelLabel(
    vector<unique_ptr<atomic_uint64_vec_t>>& directionNumRelsPerLabel, label_t labelID) {
    initCatalogContentForWriteTrxIfNecessary();
    for (auto relDirection : REL_DIRECTIONS) {
        for (auto boundNodeLabel :
            getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(labelID, relDirection)) {
            catalogContentForWriteTrx->getRelLabel(labelID)
                ->numRelsPerDirectionBoundLabel[relDirection][boundNodeLabel] =
                directionNumRelsPerLabel[relDirection]->operator[](boundNodeLabel).load();
        }
    }
}

} // namespace catalog
} // namespace graphflow
