#include "src/common/include/compression_scheme.h"

#include <stdexcept>

uint64_t getNumBytes(CompressionScheme scheme) {
    switch (scheme) {
    case LABEL_0_NODEOFFSET_2_BYTES:
        return 2;
    case LABEL_0_NODEOFFSET_4_BYTES:
        return 4;
    case LABEL_0_NODEOFFSET_8_BYTES:
        return 8;
    case LABEL_1_NODEOFFSET_2_BYTES:
        return 3;
    case LABEL_1_NODEOFFSET_4_BYTES:
        return 5;
    case LABEL_1_NODEOFFSET_8_BYTES:
        return 9;
    case LABEL_2_NODEOFFSET_2_BYTES:
        return 4;
    case LABEL_2_NODEOFFSET_4_BYTES:
        return 6;
    case LABEL_2_NODEOFFSET_8_BYTES:
        return 10;
    case LABEL_4_NODEOFFSET_2_BYTES:
        return 6;
    case LABEL_4_NODEOFFSET_4_BYTES:
        return 8;
    case LABEL_4_NODEOFFSET_8_BYTES:
        return 12;
    default:
        throw std::invalid_argument("Unsupported compression scheme.");
    }
}

CompressionScheme getCompressionScheme(uint32_t numBytesPerlabel, uint32_t numBytesPerOffset) {
    switch (numBytesPerlabel) {
    case 0:
        switch (numBytesPerlabel) {
        case 2:
            return LABEL_0_NODEOFFSET_2_BYTES;
        case 4:
            return LABEL_0_NODEOFFSET_4_BYTES;
        case 8:
            return LABEL_0_NODEOFFSET_8_BYTES;
        }
    case 1:
        switch (numBytesPerlabel) {
        case 2:
            return LABEL_1_NODEOFFSET_2_BYTES;
        case 4:
            return LABEL_1_NODEOFFSET_4_BYTES;
        case 8:
            return LABEL_1_NODEOFFSET_8_BYTES;
        }
    case 2:
        switch (numBytesPerlabel) {
        case 2:
            return LABEL_2_NODEOFFSET_2_BYTES;
        case 4:
            return LABEL_2_NODEOFFSET_4_BYTES;
        case 8:
            return LABEL_2_NODEOFFSET_8_BYTES;
        }
    case 4:
        switch (numBytesPerlabel) {
        case 2:
            return LABEL_4_NODEOFFSET_2_BYTES;
        case 4:
            return LABEL_4_NODEOFFSET_4_BYTES;
        case 8:
            return LABEL_4_NODEOFFSET_8_BYTES;
        }
    default:
        throw std::invalid_argument("Unsupported num bytes per label.");
    }
}
