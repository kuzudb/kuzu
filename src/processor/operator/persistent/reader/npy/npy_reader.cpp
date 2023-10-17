#include "processor/operator/persistent/reader/npy_reader.h"

#include <fcntl.h>
#include <sys/stat.h>

#ifdef _WIN32
#include <errhandlingapi.h>
#include <handleapi.h>
#include <memoryapi.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

#include "common/exception/buffer_manager.h"
#include "common/exception/copy.h"
#include "common/string_format.h"
#include "common/utils.h"
#include "pyparse.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

NpyReader::NpyReader(const std::string& filePath) : filePath{filePath} {
    fd = open(filePath.c_str(), O_RDONLY);
    if (fd == -1) {
        throw CopyException("Failed to open NPY file.");
    }
    struct stat fileStatus {};
    fstat(fd, &fileStatus);
    fileSize = fileStatus.st_size;

#ifdef _WIN32
    DWORD low = (DWORD)(fileSize & 0xFFFFFFFFL);
    DWORD high = (DWORD)((fileSize >> 32) & 0xFFFFFFFFL);
    auto handle =
        CreateFileMappingW((HANDLE)_get_osfhandle(fd), NULL, PAGE_READONLY, high, low, NULL);
    if (handle == NULL) {
        throw BufferManagerException(
            stringFormat("CreateFileMapping for size {} failed with error code {}: {}.", fileSize,
                GetLastError(), std::system_category().message(GetLastError())));
    }

    mmapRegion = MapViewOfFile(handle, FILE_MAP_READ, 0, 0, fileSize);
    CloseHandle(handle);
    if (mmapRegion == NULL) {
        throw BufferManagerException(
            stringFormat("MapViewOfFile for size {} failed with error code {}: {}.", fileSize,
                GetLastError(), std::system_category().message(GetLastError())));
    }
#else
    mmapRegion = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
    if (mmapRegion == MAP_FAILED) {
        throw CopyException("Failed to mmap NPY file.");
    }
#endif
    parseHeader();
}

NpyReader::~NpyReader() {
#ifdef _WIN32
    UnmapViewOfFile(mmapRegion);
#else
    munmap(mmapRegion, fileSize);
#endif
    close(fd);
}

size_t NpyReader::getNumElementsPerRow() const {
    size_t numElements = 1;
    for (size_t i = 1; i < shape.size(); ++i) {
        numElements *= shape[i];
    }
    return numElements;
}

uint8_t* NpyReader::getPointerToRow(size_t row) const {
    if (row >= getNumRows()) {
        return nullptr;
    }
    return (
        uint8_t*)((char*)mmapRegion + dataOffset +
                  row * getNumElementsPerRow() * StorageUtils::getDataTypeSize(LogicalType{type}));
}

void NpyReader::parseHeader() {
    // The first 6 bytes are a magic string: exactly \x93NUMPY
    char* magicString = (char*)mmapRegion;
    const char* expectedMagicString = "\x93NUMPY";
    if (memcmp(magicString, expectedMagicString, 6) != 0) {
        throw CopyException("Invalid NPY file");
    }

    // The next 1 byte is an unsigned byte: the major version number of the file
    // format, e.g. x01.
    char* majorVersion = magicString + 6;
    if (*majorVersion != 1) {
        throw CopyException("Unsupported NPY file version.");
    }
    // The next 1 byte is an unsigned byte: the minor version number of the file
    // format, e.g. x00. Note: the version of the file format is not tied to the
    // version of the numpy package.
    char* minorVersion = majorVersion + 1;
    if (*minorVersion != 0) {
        throw CopyException("Unsupported NPY file version.");
    }
    // The next 2 bytes form a little-endian unsigned short int: the length of
    // the header data HEADER_LEN.
    auto headerLength = *(unsigned short int*)(minorVersion + 1);
    if (!isLittleEndian()) {
        headerLength = ((headerLength & 0xff00) >> 8) | ((headerLength & 0x00ff) << 8);
    }

    // The next HEADER_LEN bytes form the header data describing the array's
    // format. It is an ASCII string which contains a Python literal expression
    // of a dictionary. It is terminated by a newline ('n') and padded with
    // spaces ('x20') to make the total length of the magic string + 4 +
    // HEADER_LEN be evenly divisible by 16 for alignment purposes.
    auto metaInfoLength = strlen(expectedMagicString) + 4;
    char* header = (char*)mmapRegion + metaInfoLength;
    auto headerEnd = std::find(header, header + headerLength, '}');

    std::string headerString(header, headerEnd + 1);
    std::unordered_map<std::string, std::string> headerMap =
        pyparse::parse_dict(headerString, {"descr", "fortran_order", "shape"});
    auto isFortranOrder = pyparse::parse_bool(headerMap["fortran_order"]);
    if (isFortranOrder) {
        throw CopyException("Fortran-order NPY files are not currently supported.");
    }
    auto descr = pyparse::parse_str(headerMap["descr"]);
    parseType(descr);
    auto shapeV = pyparse::parse_tuple(headerMap["shape"]);
    for (auto const& item : shapeV) {
        shape.emplace_back(std::stoul(item));
    }
    dataOffset = metaInfoLength + headerLength;
}

void NpyReader::parseType(std::string descr) {
    if (descr[0] == '<' || descr[0] == '>') {
        // Data type endianness is specified
        auto machineEndianness = isLittleEndian() ? "<" : ">";
        if (descr[0] != machineEndianness[0]) {
            throw CopyException(
                "The endianness of the file does not match the machine's endianness.");
        }
        descr = descr.substr(1);
    }
    if (descr[0] == '|' || descr[0] == '=') {
        // Data type endianness is not applicable or native
        descr = descr.substr(1);
    }
    if (descr == "f8") {
        type = LogicalTypeID::DOUBLE;
    } else if (descr == "f4") {
        type = LogicalTypeID::FLOAT;
    } else if (descr == "i8") {
        type = LogicalTypeID::INT64;
    } else if (descr == "i4") {
        type = LogicalTypeID::INT32;
    } else if (descr == "i2") {
        type = LogicalTypeID::INT16;
    } else {
        throw CopyException("Unsupported data type: " + descr);
    }
}

void NpyReader::validate(const LogicalType& type_, offset_t numRows) {
    auto numNodesInFile = getNumRows();
    if (numNodesInFile == 0) {
        throw CopyException(stringFormat("Number of rows in npy file {} is 0.", filePath));
    }
    if (numNodesInFile != numRows) {
        throw CopyException("Number of rows in npy files is not equal to each other.");
    }
    // TODO(Guodong): Set npy reader data type to FIXED_LIST, so we can simplify checks here.
    if (type_.getLogicalTypeID() == this->type) {
        if (getNumElementsPerRow() != 1) {
            throw CopyException(stringFormat("Cannot copy a vector property in npy file {} to a "
                                             "scalar property.",
                filePath));
        }
        return;
    } else if (type_.getLogicalTypeID() == LogicalTypeID::FIXED_LIST) {
        if (this->type != FixedListType::getChildType(&type_)->getLogicalTypeID()) {
            throw CopyException(stringFormat("The type of npy file {} does not "
                                             "match the expected type.",
                filePath));
        }
        if (getNumElementsPerRow() != FixedListType::getNumElementsInList(&type_)) {
            throw CopyException(stringFormat("The shape of {} does not match the length of the "
                                             "fixed list property.",
                filePath));
        }
        return;
    } else {
        throw CopyException(stringFormat("The type of npy file {} does not "
                                         "match the expected type.",
            filePath));
    }
}

void NpyReader::readBlock(block_idx_t blockIdx, common::ValueVector* vectorToRead) const {
    uint64_t rowNumber = DEFAULT_VECTOR_CAPACITY * blockIdx;
    auto numRows = getNumRows();
    if (rowNumber >= numRows) {
        vectorToRead->state->selVector->selectedSize = 0;
    } else {
        auto rowPointer = getPointerToRow(rowNumber);
        auto numRowsToRead = std::min(DEFAULT_VECTOR_CAPACITY, getNumRows() - rowNumber);
        memcpy(vectorToRead->getData(), rowPointer,
            numRowsToRead * vectorToRead->getNumBytesPerValue());
        vectorToRead->state->selVector->selectedSize = numRowsToRead;
    }
}

NpyMultiFileReader::NpyMultiFileReader(const std::vector<std::string>& filePaths) {
    for (auto& file : filePaths) {
        fileReaders.push_back(std::make_unique<NpyReader>(file));
    }
}

void NpyMultiFileReader::readBlock(block_idx_t blockIdx, common::DataChunk* dataChunkToRead) const {
    for (auto i = 0u; i < fileReaders.size(); i++) {
        fileReaders[i]->readBlock(blockIdx, dataChunkToRead->getValueVector(i).get());
    }
}

} // namespace processor
} // namespace kuzu
