#pragma once

#include <map>
#include <memory>

#include "test/storage/include/file_scanners/file_scanner.h"
#include "test/storage/include/file_scanners/string_overflow_file_scanner.h"

#include "src/common/include/literal.h"
#include "src/common/include/types.h"
#include "src/storage/include/store/nodes_store.h"

namespace graphflow {
namespace storage {

class UnstructuredNodePropertyFileScanner : FileScanner {
public:
    UnstructuredNodePropertyFileScanner(
        const string& directory, const graphflow::common::label_t& nodeLabel)
        : FileScanner{NodesStore::getNodeUnstrPropertyListsFName(directory, nodeLabel)},
          nodeLabel{nodeLabel}, catalog{directory},
          listHeaders{NodesStore::getNodeUnstrPropertyListsFName(directory, nodeLabel)},
          listsMetadata{NodesStore::getNodeUnstrPropertyListsFName(directory, nodeLabel)},
          stringOverflowFileScanner(StringOverflowPages::getStringOverflowPagesFName(
              NodesStore::getNodeUnstrPropertyListsFName(directory, nodeLabel))) {}

    static inline PageCursor getPageCursorForOffset(const uint64_t& offset) {
        return PageCursor{offset >> PAGE_SIZE_LOG_2 /* num elements per page */,
            (uint16_t)((offset & (PAGE_SIZE - 1) /* num elements per page */) *
                       1 /* element size in bytes */)};
    }

    void readUnstrPropertyKeyIdxAndDatatype(uint64_t& physicalPageIdx,
        uint8_t* propertyKeyDataTypeCache, const uint32_t*& propertyKeyIdxPtr,
        DataType& propertyDataType, PageCursor& pageCursor, uint64_t& listLen,
        const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper);

    Literal readUnstrPropertyValue(uint64_t& physicalPageIdx, DataType& propertyDataType,
        PageCursor& pageCursor, uint64_t& listLen,
        const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper);

    map<string, Literal> readUnstructuredProperties(int nodeOffset);

private:
    label_t nodeLabel;
    Catalog catalog;
    ListHeaders listHeaders;
    ListsMetadata listsMetadata;
    StringOverflowFileFileScanner stringOverflowFileScanner;
};

} // namespace storage
} // namespace graphflow
