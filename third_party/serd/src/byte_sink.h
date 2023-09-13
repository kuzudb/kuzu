// Copyright 2011-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#ifndef SERD_SRC_BYTE_SINK_H
#define SERD_SRC_BYTE_SINK_H

#include "serd_internal.h"
#include "system.h"

#include "serd.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

typedef struct SerdByteSinkImpl {
  SerdSink sink;
  void*    stream;
  uint8_t* buf;
  size_t   size;
  size_t   block_size;
} SerdByteSink;

static inline SerdByteSink
serd_byte_sink_new(SerdSink sink, void* stream, size_t block_size)
{
  SerdByteSink bsink = {sink, stream, NULL, 0, block_size};

  if (block_size > 1) {
    bsink.buf = (uint8_t*)serd_allocate_buffer(block_size);
  }

  return bsink;
}

static inline SerdStatus
serd_byte_sink_flush(SerdByteSink* bsink)
{
  if (bsink->block_size > 1 && bsink->size > 0) {
    const size_t size  = bsink->size;
    const size_t n_out = bsink->sink(bsink->buf, size, bsink->stream);
    bsink->size        = 0;

    return (n_out != size) ? SERD_ERR_BAD_WRITE : SERD_SUCCESS;
  }

  return SERD_SUCCESS;
}

static inline void
serd_byte_sink_free(SerdByteSink* bsink)
{
  serd_byte_sink_flush(bsink);
  serd_free_aligned(bsink->buf);
  bsink->buf = NULL;
}

static inline size_t
serd_byte_sink_write(const void* buf, size_t len, SerdByteSink* bsink)
{
  if (len == 0) {
    return 0;
  }

  if (bsink->block_size == 1) {
    return bsink->sink(buf, len, bsink->stream);
  }

  const size_t orig_len = len;
  while (len) {
    const size_t space = bsink->block_size - bsink->size;
    const size_t n     = MIN(space, len);

    // Write as much as possible into the remaining buffer space
    memcpy(bsink->buf + bsink->size, buf, n);
    bsink->size += n;
    buf = (const uint8_t*)buf + n;
    len -= n;

    // Flush page if buffer is full
    if (bsink->size == bsink->block_size) {
      bsink->sink(bsink->buf, bsink->block_size, bsink->stream);
      bsink->size = 0;
    }
  }

  return orig_len;
}

#endif // SERD_SRC_BYTE_SINK_H
