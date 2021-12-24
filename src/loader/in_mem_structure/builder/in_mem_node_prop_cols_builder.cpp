#include "src/loader/include/in_mem_structure/builder/in_mem_node_prop_cols_builder.h"

#include "src/loader/include/in_mem_structure/in_mem_pages.h"
#include "src/loader/include/loader_task.h"

namespace graphflow {
namespace loader {

InMemNodePropertyColumnsBuilder::InMemNodePropertyColumnsBuilder(NodeLabelDescription& description,
    TaskScheduler& taskScheduler, const Graph& graph, string outputDirectory)
    : InMemStructuresBuilder(taskScheduler, graph, move(outputDirectory)),
      description(description) {
    buildInMemPropertyColumns();
}

void InMemNodePropertyColumnsBuilder::buildInMemPropertyColumns() {
    logger->debug("Creating InMemProperty Columns.");
    propertyIdxPropertyColumn.resize(description.properties.size());
    propertyIdxStringOverflowPages.resize(description.properties.size());
    for (auto& property : description.properties) {
        auto fName = NodesStore::getNodePropertyColumnFName(
            outputDirectory, description.label, property.name);
        auto numPages = calcNumPagesInColumn(TypeUtils::getDataTypeSize(property.dataType),
            graph.getNumNodesPerLabel()[description.label]);
        propertyIdxPropertyColumn[property.id] = make_unique<InMemPropertyPages>(
            fName, TypeUtils::getDataTypeSize(property.dataType), numPages);
        if (STRING == property.dataType) {
            propertyIdxStringOverflowPages[property.id] = make_unique<InMemStringOverflowPages>(
                StringOverflowPages::getStringOverflowPagesFName(fName));
        }
    }
    logger->debug("Done creating InMemProperty Columns.");
}

void InMemNodePropertyColumnsBuilder::setProperty(node_offset_t nodeOffset,
    const uint32_t& propertyIdx, const uint8_t* val, const DataType& type) {
    PageElementCursor cursor;
    calcPageElementCursor(TypeUtils::getDataTypeSize(type), nodeOffset, cursor);
    propertyIdxPropertyColumn[propertyIdx]->set(cursor, val);
}

void InMemNodePropertyColumnsBuilder::setStringProperty(node_offset_t nodeOffset,
    const uint32_t& propertyIdx, const char* originalString, PageByteCursor& cursor) {
    gf_string_t gfString;
    propertyIdxStringOverflowPages[propertyIdx]->setStrInOvfPageAndPtrInEncString(
        originalString, cursor, &gfString);
    setProperty(nodeOffset, propertyIdx, reinterpret_cast<uint8_t*>(&gfString), STRING);
}

void InMemNodePropertyColumnsBuilder::saveToFile() {
    logger->debug("Writing Node Property Columns to disk.");
    for (auto& property : description.properties) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
            [&](InMemPropertyPages* x) { x->saveToFile(); },
            reinterpret_cast<InMemPropertyPages*>(propertyIdxPropertyColumn[property.id].get())));
        if (STRING == property.dataType) {
            taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                [&](InMemStringOverflowPages* x) { x->saveToFile(); },
                (propertyIdxStringOverflowPages)[property.id].get()));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done writing Node Property Columns to disk.");
}

} // namespace loader
} // namespace graphflow
