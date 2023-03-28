#include "storage/copier/npy_reader.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pyparse.h"
#include <common/utils.h>

namespace kuzu {
namespace storage {
NpyReader::NpyReader(const std::string& filePath) : filePath{filePath} {
    fd = open(filePath.c_str(), O_RDONLY);
    if (fd == -1) {
        throw common::Exception("Failed to open NPY file.");
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
    if (row >= getLength()) {
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
        throw common::Exception("Invalid NPY file");
    }

    // The next 1 byte is an unsigned byte: the major version number of the file
    // format, e.g. x01.
    char* majorVersion = magicString + 6;
    if (*majorVersion != 1) {
        throw common::Exception("Unsupported NPY file version.");
    }
    // The next 1 byte is an unsigned byte: the minor version number of the file
    // format, e.g. x00. Note: the version of the file format is not tied to the
    // version of the numpy package.
    char* minorVersion = majorVersion + 1;
    if (*minorVersion != 0) {
        throw common::Exception("Unsupported NPY file version.");
    }
    // The next 2 bytes form a little-endian unsigned short int: the length of
    // the header data HEADER_LEN.
    auto headerLength = *(unsigned short int*)(minorVersion + 1);
    if (!common::isLittleEndian()) {
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
        throw common::Exception("Fortran-order NPY files are not currently supported.");
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
        auto machineEndianness = common::isLittleEndian() ? "<" : ">";
        if (descr[0] != machineEndianness[0]) {
            throw common::Exception(
                "The endianness of the file does not match the machine's endianness.");
        }
        descr = descr.substr(1);
    }
    if (descr[0] == '|' || descr[0] == '=') {
        // Data type endianness is not applicable or native
        descr = descr.substr(1);
    }
    if (descr == "f8") {
        type = common::DOUBLE;
    } else if (descr == "f4") {
        type = common::FLOAT;
    } else if (descr == "i8") {
        type = common::INT64;
    } else if (descr == "i4") {
        type = common::INT32;
    } else if (descr == "i2") {
        type = common::INT16;
    } else {
        throw common::Exception("Unsupported data type: " + descr);
    }
}
} // namespace storage
} // namespace kuzu
