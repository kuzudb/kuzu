#pragma once

#include <stdint.h>

enum CompressionScheme {
    LABEL_0_NODEOFFSET_2_BYTES,
    LABEL_0_NODEOFFSET_4_BYTES,
    LABEL_0_NODEOFFSET_8_BYTES,
    LABEL_1_NODEOFFSET_2_BYTES,
    LABEL_1_NODEOFFSET_4_BYTES,
    LABEL_1_NODEOFFSET_8_BYTES,
    LABEL_2_NODEOFFSET_2_BYTES,
    LABEL_2_NODEOFFSET_4_BYTES,
    LABEL_2_NODEOFFSET_8_BYTES,
    LABEL_4_NODEOFFSET_2_BYTES,
    LABEL_4_NODEOFFSET_4_BYTES,
    LABEL_4_NODEOFFSET_8_BYTES,
};

uint64_t getNumBytes(CompressionScheme compressionScheme);

CompressionScheme getCompressionScheme(uint32_t numBytesPerlabel, uint32_t numBytesPerOffset);
