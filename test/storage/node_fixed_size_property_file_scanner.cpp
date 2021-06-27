#include "test/storage/include/node_fixed_size_property_file_scanner.h"

#include "src/common/include/file_utils.h"
#include "src/storage/include/store/nodes_store.h"

using namespace std;

namespace graphflow {
namespace storage {

NodeFixedSizePropertyFileScanner::NodeFixedSizePropertyFileScanner(const string& directory,
    const graphflow::common::label_t& nodeLabel, const string& propertyName) {
    string fName = NodesStore::getNodePropertyColumnFname(directory, nodeLabel, propertyName);
    int fd = FileUtils::openFile(fName, O_RDONLY);
    auto length = FileUtils::getFileSize(fd);
    buffer = make_unique<char[]>(length);
    FileUtils::readFromFile(fd, buffer.get(), length, 0);
    FileUtils::closeFile(fd);
}

} // namespace storage
} // namespace graphflow