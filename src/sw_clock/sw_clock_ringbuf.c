/**
 * @file sw_clock_ringbuf.c
 * @brief Lock-free ring buffer implementation
 */

#include "sw_clock_ringbuf.h"
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <syslog.h>

static void swclock_ringbuf_log(int priority, const char *format, ...) {
    char message[256];
    va_list ap;
    FILE *sink = NULL;

    (void)priority;

    va_start(ap, format);
    vsnprintf(message, sizeof(message), format, ap);
    va_end(ap);

    sink = fopen("logs/swclock_internal.log", "a");
    if (!sink) {
        sink = fopen("/tmp/swclock_internal.log", "a");
    }
    if (!sink) {
        return;
    }

    fprintf(sink, "[swclock_ringbuf] %s\n", message);
    fclose(sink);
}

void swclock_ringbuf_init(swclock_ringbuf_t* rb) {
    if (!rb) return;

    memset(rb, 0, sizeof(*rb));
    atomic_init(&rb->write_pos, 0);
    atomic_init(&rb->read_pos, 0);
    atomic_init(&rb->overrun_flag, false);
}

bool swclock_ringbuf_push(
    swclock_ringbuf_t* rb,
    const void* data,
    size_t size)
{
    if (!rb || !data || size == 0 || size > SWCLOCK_RINGBUF_SIZE / 2) {
        return false;
    }

    // Read current positions (atomic)
    uint64_t write_pos = atomic_load_explicit(&rb->write_pos, memory_order_acquire);
    uint64_t read_pos = atomic_load_explicit(&rb->read_pos, memory_order_acquire);

    // Calculate available space
    uint64_t used = write_pos - read_pos;
    uint64_t available = SWCLOCK_RINGBUF_SIZE - used;

    // Need space for: 4-byte size header + data
    size_t total_size = sizeof(uint32_t) + size;

    if (available < total_size) {
        // Buffer full - set overrun flag
        atomic_store_explicit(&rb->overrun_flag, true, memory_order_release);
        rb->overrun_count++;
        return false;
    }

    // Write size header (4 bytes)
    uint32_t size_header = (uint32_t)size;
    size_t pos = write_pos % SWCLOCK_RINGBUF_SIZE;

    // Handle wrap-around for size header
    if (pos + sizeof(uint32_t) <= SWCLOCK_RINGBUF_SIZE) {
        memcpy(&rb->buffer[pos], &size_header, sizeof(uint32_t));
    } else {
        // Wrap around
        size_t first_part = SWCLOCK_RINGBUF_SIZE - pos;
        memcpy(&rb->buffer[pos], &size_header, first_part);
        memcpy(&rb->buffer[0], ((uint8_t*)&size_header) + first_part,
               sizeof(uint32_t) - first_part);
    }

    pos = (write_pos + sizeof(uint32_t)) % SWCLOCK_RINGBUF_SIZE;

    // Write data with wrap-around handling
    if (pos + size <= SWCLOCK_RINGBUF_SIZE) {
        memcpy(&rb->buffer[pos], data, size);
    } else {
        // Wrap around
        size_t first_part = SWCLOCK_RINGBUF_SIZE - pos;
        memcpy(&rb->buffer[pos], data, first_part);
        memcpy(&rb->buffer[0], ((uint8_t*)data) + first_part, size - first_part);
    }

    // Update write position (atomic)
    atomic_store_explicit(&rb->write_pos, write_pos + total_size, memory_order_release);
    rb->events_written++;

    return true;
}

bool swclock_ringbuf_pop(
    swclock_ringbuf_t* rb,
    void* data,
    size_t max_size,
    size_t* actual_size)
{
    if (!rb || !data || max_size == 0) {
        return false;
    }

    // Read current positions (atomic)
    uint64_t write_pos = atomic_load_explicit(&rb->write_pos, memory_order_acquire);
    uint64_t read_pos = atomic_load_explicit(&rb->read_pos, memory_order_acquire);

    // Check if empty
    if (write_pos == read_pos) {
        return false;
    }

    // Read size header (4 bytes)
    uint32_t size_header;
    size_t pos = read_pos % SWCLOCK_RINGBUF_SIZE;

    // Handle wrap-around for size header
    if (pos + sizeof(uint32_t) <= SWCLOCK_RINGBUF_SIZE) {
        memcpy(&size_header, &rb->buffer[pos], sizeof(uint32_t));
    } else {
        // Wrap around
        size_t first_part = SWCLOCK_RINGBUF_SIZE - pos;
        memcpy(&size_header, &rb->buffer[pos], first_part);
        memcpy(((uint8_t*)&size_header) + first_part, &rb->buffer[0],
               sizeof(uint32_t) - first_part);
    }

    // Validate size
    if (size_header == 0 || size_header > max_size) {
        // Corrupted data or buffer too small
        swclock_ringbuf_log(LOG_WARNING,
                    "Invalid size %u (max %zu)",
                    size_header, max_size);
        return false;
    }

    if (actual_size) {
        *actual_size = size_header;
    }

    pos = (read_pos + sizeof(uint32_t)) % SWCLOCK_RINGBUF_SIZE;

    // Read data with wrap-around handling
    if (pos + size_header <= SWCLOCK_RINGBUF_SIZE) {
        memcpy(data, &rb->buffer[pos], size_header);
    } else {
        // Wrap around
        size_t first_part = SWCLOCK_RINGBUF_SIZE - pos;
        memcpy(data, &rb->buffer[pos], first_part);
        memcpy(((uint8_t*)data) + first_part, &rb->buffer[0],
               size_header - first_part);
    }

    // Update read position (atomic)
    atomic_store_explicit(&rb->read_pos,
                         read_pos + sizeof(uint32_t) + size_header,
                         memory_order_release);
    rb->events_read++;

    return true;
}

bool swclock_ringbuf_is_empty(const swclock_ringbuf_t* rb) {
    if (!rb) return true;

    uint64_t write_pos = atomic_load_explicit(&rb->write_pos, memory_order_acquire);
    uint64_t read_pos = atomic_load_explicit(&rb->read_pos, memory_order_acquire);

    return write_pos == read_pos;
}

size_t swclock_ringbuf_available(const swclock_ringbuf_t* rb) {
    if (!rb) return 0;

    uint64_t write_pos = atomic_load_explicit(&rb->write_pos, memory_order_acquire);
    uint64_t read_pos = atomic_load_explicit(&rb->read_pos, memory_order_acquire);

    uint64_t used = write_pos - read_pos;
    return SWCLOCK_RINGBUF_SIZE - (size_t)used;
}

size_t swclock_ringbuf_used(const swclock_ringbuf_t* rb) {
    if (!rb) return 0;

    uint64_t write_pos = atomic_load_explicit(&rb->write_pos, memory_order_acquire);
    uint64_t read_pos = atomic_load_explicit(&rb->read_pos, memory_order_acquire);

    return (size_t)(write_pos - read_pos);
}

bool swclock_ringbuf_clear_overrun(swclock_ringbuf_t* rb) {
    if (!rb) return false;

    return atomic_exchange_explicit(&rb->overrun_flag, false, memory_order_acq_rel);
}

void swclock_ringbuf_stats(
    const swclock_ringbuf_t* rb,
    uint64_t* events_written,
    uint64_t* events_read,
    uint64_t* overrun_count)
{
    if (!rb) return;

    if (events_written) *events_written = rb->events_written;
    if (events_read) *events_read = rb->events_read;
    if (overrun_count) *overrun_count = rb->overrun_count;
}
