#pragma once

#include <memory>

#include "test/storage/include/file_scanners/file_scanner.h"
#include "test/storage/include/file_scanners/string_overflow_file_scanner.h"

#include "src/common/include/types.h"
#include "src/storage/include/store/nodes_store.h"

namespace graphflow {
namespace storage {

class StructuredNodePropertyFileScanner : FileScanner {

public:
    StructuredNodePropertyFileScanner(const string& directory,
        const graphflow::common::label_t& nodeLabel, const string& propertyName)
        : FileScanner(NodesStore::getNodePropertyColumnFname(directory, nodeLabel, propertyName)) {}

    template<typename T>
    T readProperty(int nodeOffset) {
        auto offset = nodeOffset * sizeof(T);
        return *(T*)(buffer.get() + offset);
    };
};

class StructuredStringNodePropertyFileScanner {

public:
    StructuredStringNodePropertyFileScanner(const string& directory,
        const graphflow::common::label_t& nodeLabel, const string& propertyName)
        : structuredNodePropertyFileScanner(directory, nodeLabel, propertyName),
          stringOverflowFileScanner(StringOverflowPages::getStringOverflowPagesFName(
              NodesStore::getNodePropertyColumnFname(directory, nodeLabel, propertyName))) {}

    string readString(int nodeOffset) {
        gf_string_t mainString =
            structuredNodePropertyFileScanner.readProperty<gf_string_t>(nodeOffset);
        return StringOverflowFileFileScanner::readString(mainString, stringOverflowFileScanner);
    };

private:
    StructuredNodePropertyFileScanner structuredNodePropertyFileScanner;
    StringOverflowFileFileScanner stringOverflowFileScanner;
};

} // namespace storage
} // namespace graphflow
