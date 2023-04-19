#include "storage/copier/npy_reader.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/exception.h"
#include "common/string_utils.h"
#include "common/utils.h"
#include "pyparse.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

NpyReader::NpyReader(const std::string& filePath) : filePath{filePath} {
    fd = open(filePath.c_str(), O_RDONLY);
    if (fd == -1) {
        throw CopyException("Failed to open NPY file.");
    }
    struct stat fileStatus {};
    fstat(fd, &fileStatus);
    fileSize = fileStatus.st_size;
    mmapRegion = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
    if (mmapRegion == MAP_FAILED) {
        throw common::Exception("Failed to mmap NPY file.");
    }
    parseHeader();
}

NpyReader::~NpyReader() {
    munmap(mmapRegion, fileSize);
    close(fd);
}

size_t NpyReader::getNumElementsPerRow() const {
    size_t numElements = 1;
    for (size_t i = 1; i < shape.size(); ++i) {
        numElements *= shape[i];
    }
    return numElements;
}

void* NpyReader::getPointerToRow(size_t row) const {
    if (row >= getNumRows()) {
        return nullptr;
    }
    return (void*)((char*)mmapRegion + dataOffset +
                   row * getNumElementsPerRow() * common::Types::getDataTypeSize(type));
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
        type = DOUBLE;
    } else if (descr == "f4") {
        type = FLOAT;
    } else if (descr == "i8") {
        type = INT64;
    } else if (descr == "i4") {
        type = INT32;
    } else if (descr == "i2") {
        type = INT16;
    } else {
        throw CopyException("Unsupported data type: " + descr);
    }
}

void NpyReader::validate(DataType& type_, offset_t numRows, const std::string& tableName) {
    auto numNodesInFile = getNumRows();
    if (numNodesInFile == 0) {
        throw CopyException(
            StringUtils::string_format("Number of rows in npy file {} is 0.", filePath));
    }
    if (numNodesInFile != numRows) {
        throw CopyException("Number of rows in npy files is not equal to each other.");
    }
    // TODO(Guodong): Set npy reader data type to FIXED_LIST, so we can simplify checks here.
    if (type_.typeID == this->type) {
        if (getNumElementsPerRow() != 1) {
            throw CopyException(
                StringUtils::string_format("Cannot copy a vector property in npy file {} to a "
                                           "scalar property in table {}.",
                    filePath, tableName));
        }
        return;
    } else if (type_.typeID == DataTypeID::FIXED_LIST) {
        if (this->type != type_.getChildType()->typeID) {
            throw CopyException(StringUtils::string_format("The type of npy file {} does not "
                                                           "match the type defined in table {}.",
                filePath, tableName));
        }
        auto fixedListInfo = reinterpret_cast<FixedListTypeInfo*>(type_.getExtraTypeInfo());
        if (getNumElementsPerRow() != fixedListInfo->getFixedNumElementsInList()) {
            throw CopyException(
                StringUtils::string_format("The shape of {} does not match the length of the "
                                           "fixed list property in table "
                                           "{}.",
                    filePath, tableName));
        }
        return;
    } else {
        throw CopyException(StringUtils::string_format("The type of npy file {} does not "
                                                       "match the type defined in table {}.",
            filePath, tableName));
    }
}
} // namespace storage
} // namespace kuzu
