#include "src/loader/include/in_mem_structure/builder/in_mem_node_prop_cols_builder.h"

#include "src/loader/include/in_mem_structure/in_mem_pages.h"

namespace graphflow {
namespace loader {

InMemNodePropertyColumnsBuilder::InMemNodePropertyColumnsBuilder(NodeLabelDescription& description,
    ThreadPool& threadPool, const Graph& graph, string outputDirectory)
    : InMemStructuresBuilder(threadPool, graph, move(outputDirectory)), description(description) {
    buildInMemPropertyColumns();
}

void InMemNodePropertyColumnsBuilder::buildInMemPropertyColumns() {
    logger->debug("Creating InMemProperty Columns.");
    propertyIdxPropertyColumn.resize(description.properties.size());
    propertyIdxStringOverflowPages.resize(description.properties.size());
    auto numElements = graph.getNumNodesPerLabel()[description.label];
    for (auto& property : description.properties) {
        auto fName = NodesStore::getNodePropertyColumnFName(
            outputDirectory, description.label, property.name);
        uint32_t numElementsPerPage = PAGE_SIZE / TypeUtils::getDataTypeSize(property.dataType);
        uint64_t numPages = numElements / numElementsPerPage;
        if (0 != numElements % numElementsPerPage) {
            numPages++;
        }
        propertyIdxPropertyColumn[property.id] = make_unique<InMemPropertyPages>(
            fName, numPages, TypeUtils::getDataTypeSize(property.dataType));
        if (STRING == property.dataType) {
            propertyIdxStringOverflowPages[property.id] = make_unique<InMemStringOverflowPages>(
                StringOverflowPages::getStringOverflowPagesFName(fName));
        }
    }
    logger->debug("Done creating InMemProperty Columns.");
}

void InMemNodePropertyColumnsBuilder::setProperty(node_offset_t nodeOffset,
    const uint32_t& propertyIdx, const uint8_t* val, const DataType& type) {
    PageCursor cursor;
    calculatePageCursor(TypeUtils::getDataTypeSize(type), nodeOffset, cursor);
    propertyIdxPropertyColumn[propertyIdx]->setPorperty(cursor, val);
}

void InMemNodePropertyColumnsBuilder::setStringProperty(node_offset_t nodeOffset,
    const uint32_t& propertyIdx, const char* originalString, PageCursor& cursor) {
    gf_string_t gfString;
    propertyIdxStringOverflowPages[propertyIdx]->setStrInOvfPageAndPtrInEncString(
        originalString, cursor, &gfString);
    setProperty(nodeOffset, propertyIdx, reinterpret_cast<uint8_t*>(&gfString), STRING);
}

void InMemNodePropertyColumnsBuilder::saveToFile() {
    logger->debug("Writing Node Property Columns to disk.");
    for (auto& property : description.properties) {
        threadPool.execute([&](InMemPropertyPages* x) { x->saveToFile(); },
            reinterpret_cast<InMemPropertyPages*>(propertyIdxPropertyColumn[property.id].get()));
        if (STRING == property.dataType) {
            threadPool.execute([&](InMemStringOverflowPages* x) { x->saveToFile(); },
                (propertyIdxStringOverflowPages)[property.id].get());
        }
    }
    threadPool.wait();
    logger->debug("Done writing Node Property Columns to disk.");
}

} // namespace loader
} // namespace graphflow
