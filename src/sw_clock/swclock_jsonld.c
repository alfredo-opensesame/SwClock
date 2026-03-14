/**
 * @file swclock_jsonld.c
 * @brief JSON-LD structured logging for SwClock with IEEE 1588 compliance
 *
 * Implements SwClock Interchange Format (SIF) v1.0.0 with:
 * - JSON-LD context with IEEE 1588 vocabulary
 * - Thread-safe buffered I/O (1MB buffer)
 * - Log rotation by size/time with gzip compression
 * - ISO 8601 timestamps with nanosecond precision
 *
 * Part of IEEE Audit Recommendation 10: Log Format Standardization
 */

#include "swclock_jsonld.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <zlib.h>
#include <stdarg.h>
#include <syslog.h>

static void swclock_jsonld_log(int priority, const char *format, ...) {
    char message[1024];
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

    fprintf(sink, "[swclock_jsonld] %s\n", message);
    fclose(sink);
}

#define SWCLOCK_JSONLD_ERROR(fmt, ...)                                         \
    swclock_jsonld_log(LOG_ERR, fmt, ##__VA_ARGS__)

/* Internal logger context */
struct swclock_jsonld_logger {
    FILE* fp;                              /* Log file handle */
    char log_path[512];                    /* Current log file path */
    char* buffer;                          /* Write buffer */
    size_t buffer_pos;                     /* Current buffer position */
    size_t buffer_size;                    /* Buffer size (1MB) */
    pthread_mutex_t lock;                  /* Thread safety */
    swclock_system_context_t system;       /* System metadata */
    swclock_log_rotation_t rotation;       /* Rotation config */
    uint64_t entry_count;                  /* Entries written */
    time_t created_at;                     /* File creation time */
    size_t current_size;                   /* Approximate file size */
};

/* Forward declarations */
static int detect_system_context(swclock_system_context_t* ctx);
static int format_iso8601_ns(uint64_t timestamp_ns, char* buf, size_t buflen);
static void json_escape_string(const char* src, char* dst, size_t dst_len);
static int write_jsonld_entry(swclock_jsonld_logger_t* logger, const char* entry);
static int flush_buffer(swclock_jsonld_logger_t* logger);
static int should_rotate(swclock_jsonld_logger_t* logger);
static int perform_rotation(swclock_jsonld_logger_t* logger);
static int compress_file(const char* src_path);

/* ========================================================================
 * Lifecycle Functions
 * ======================================================================== */

swclock_jsonld_logger_t* swclock_jsonld_init(
    const char* log_path,
    const swclock_log_rotation_t* rotation,
    const swclock_system_context_t* system_ctx)
{
    if (!log_path) {
        SWCLOCK_JSONLD_ERROR("swclock_jsonld_init: log_path is NULL");
        return NULL;
    }

    swclock_jsonld_logger_t* logger = calloc(1, sizeof(swclock_jsonld_logger_t));
    if (!logger) {
        SWCLOCK_JSONLD_ERROR("swclock_jsonld_init: Failed to allocate logger");
        return NULL;
    }

    /* Copy configuration */
    strncpy(logger->log_path, log_path, sizeof(logger->log_path) - 1);

    if (rotation) {
        logger->rotation = *rotation;
    } else {
        /* Default: 100MB, 7 days, 10 files, compressed */
        logger->rotation.enabled = true;
        logger->rotation.max_size_mb = 100;
        logger->rotation.max_age_hours = 168; /* 7 days */
        logger->rotation.max_files = 10;
        logger->rotation.compress = true;
    }

    /* Detect or copy system context */
    if (system_ctx) {
        logger->system = *system_ctx;
    } else {
        if (detect_system_context(&logger->system) != 0) {
            SWCLOCK_JSONLD_ERROR("swclock_jsonld_init: Failed to detect system context");
            free(logger);
            return NULL;
        }
    }

    /* Allocate write buffer */
    logger->buffer_size = SWCLOCK_JSONLD_BUFFER_SIZE;
    logger->buffer = malloc(logger->buffer_size);
    if (!logger->buffer) {
        SWCLOCK_JSONLD_ERROR("swclock_jsonld_init: Failed to allocate buffer");
        free(logger);
        return NULL;
    }
    logger->buffer_pos = 0;

    /* Initialize mutex */
    if (pthread_mutex_init(&logger->lock, NULL) != 0) {
        SWCLOCK_JSONLD_ERROR("swclock_jsonld_init: Failed to initialize mutex");
        free(logger->buffer);
        free(logger);
        return NULL;
    }

    /* Open log file (append mode for JSONL) */
    logger->fp = fopen(log_path, "a");
    if (!logger->fp) {
        SWCLOCK_JSONLD_ERROR("swclock_jsonld_init: Failed to open %s: %s",
                     log_path, strerror(errno));
        pthread_mutex_destroy(&logger->lock);
        free(logger->buffer);
        free(logger);
        return NULL;
    }

    /* Get current file size */
    struct stat st;
    if (fstat(fileno(logger->fp), &st) == 0) {
        logger->current_size = st.st_size;
    }

    logger->created_at = time(NULL);
    logger->entry_count = 0;

    return logger;
}

void swclock_jsonld_close(swclock_jsonld_logger_t* logger)
{
    if (!logger) {
        return;
    }

    /* Flush remaining buffer */
    pthread_mutex_lock(&logger->lock);
    flush_buffer(logger);

    if (logger->fp) {
        fclose(logger->fp);
        logger->fp = NULL;
    }
    pthread_mutex_unlock(&logger->lock);

    /* Cleanup */
    pthread_mutex_destroy(&logger->lock);
    free(logger->buffer);
    free(logger);
}

int swclock_jsonld_flush(swclock_jsonld_logger_t* logger)
{
    if (!logger) {
        return -1;
    }

    pthread_mutex_lock(&logger->lock);
    int ret = flush_buffer(logger);
    pthread_mutex_unlock(&logger->lock);

    return ret;
}

/* ========================================================================
 * Event Logging Functions
 * ======================================================================== */

int swclock_jsonld_log_servo(
    swclock_jsonld_logger_t* logger,
    uint64_t timestamp_mono_ns,
    double freq_ppm,
    int64_t phase_error_ns,
    int64_t time_error_ns,
    double pi_freq_ppm,
    double pi_int_error_s,
    bool servo_enabled)
{
    if (!logger) {
        return -1;
    }

    char entry[SWCLOCK_JSONLD_MAX_SIZE];
    char ts_buf[64];
    format_iso8601_ns(timestamp_mono_ns, ts_buf, sizeof(ts_buf));

    snprintf(entry, sizeof(entry),
        "{"
        "\"@context\":{\"@vocab\":\"https://swclock.org/vocab#\","
        "\"ieee1588\":\"https://standards.ieee.org/1588/vocab#\"},"
        "\"@type\":\"ServoStateUpdate\","
        "\"timestamp\":\"%s\","
        "\"timestamp_monotonic_ns\":%llu,"
        "\"event\":{"
        "\"freq_ppm\":%.6f,"
        "\"phase_error_ns\":%ld,"
        "\"time_error_ns\":%ld,"
        "\"pi_freq_ppm\":%.6f,"
        "\"pi_int_error_s\":%.12f,"
        "\"servo_enabled\":%s"
        "},"
        "\"system\":{"
        "\"hostname\":\"%s\","
        "\"os\":\"%s\","
        "\"kernel\":\"%s\","
        "\"arch\":\"%s\","
        "\"swclock_version\":\"%s\""
        "}"
        "}\n",
        ts_buf,
        (unsigned long long)timestamp_mono_ns,
        freq_ppm,
        (long)phase_error_ns,
        (long)time_error_ns,
        pi_freq_ppm,
        pi_int_error_s,
        servo_enabled ? "true" : "false",
        logger->system.hostname,
        logger->system.os,
        logger->system.kernel,
        logger->system.arch,
        logger->system.swclock_version);

    return write_jsonld_entry(logger, entry);
}

int swclock_jsonld_log_adjustment(
    swclock_jsonld_logger_t* logger,
    uint64_t timestamp_mono_ns,
    const char* adjustment_type,
    double value,
    int64_t before_offset_ns,
    int64_t after_offset_ns)
{
    if (!logger || !adjustment_type) {
        return -1;
    }

    char entry[SWCLOCK_JSONLD_MAX_SIZE];
    char ts_buf[64];
    char escaped_type[128];

    format_iso8601_ns(timestamp_mono_ns, ts_buf, sizeof(ts_buf));
    json_escape_string(adjustment_type, escaped_type, sizeof(escaped_type));

    snprintf(entry, sizeof(entry),
        "{"
        "\"@context\":{\"@vocab\":\"https://swclock.org/vocab#\"},"
        "\"@type\":\"TimeAdjustment\","
        "\"timestamp\":\"%s\","
        "\"timestamp_monotonic_ns\":%llu,"
        "\"event\":{"
        "\"adjustment_type\":\"%s\","
        "\"value\":%.6f,"
        "\"before_offset_ns\":%ld,"
        "\"after_offset_ns\":%ld"
        "}"
        "}\n",
        ts_buf,
        (unsigned long long)timestamp_mono_ns,
        escaped_type,
        value,
        (long)before_offset_ns,
        (long)after_offset_ns);

    return write_jsonld_entry(logger, entry);
}

int swclock_jsonld_log_pi_update(
    swclock_jsonld_logger_t* logger,
    uint64_t timestamp_mono_ns,
    double kp,
    double ki,
    double error_s,
    double output_ppm,
    double integral_state)
{
    if (!logger) {
        return -1;
    }

    char entry[SWCLOCK_JSONLD_MAX_SIZE];
    char ts_buf[64];
    format_iso8601_ns(timestamp_mono_ns, ts_buf, sizeof(ts_buf));

    snprintf(entry, sizeof(entry),
        "{"
        "\"@context\":{\"@vocab\":\"https://swclock.org/vocab#\"},"
        "\"@type\":\"PIUpdate\","
        "\"timestamp\":\"%s\","
        "\"timestamp_monotonic_ns\":%llu,"
        "\"event\":{"
        "\"kp\":%.3f,"
        "\"ki\":%.3f,"
        "\"error_s\":%.12f,"
        "\"output_ppm\":%.6f,"
        "\"integral_state\":%.12f"
        "}"
        "}\n",
        ts_buf,
        (unsigned long long)timestamp_mono_ns,
        kp,
        ki,
        error_s,
        output_ppm,
        integral_state);

    return write_jsonld_entry(logger, entry);
}

int swclock_jsonld_log_alert(
    swclock_jsonld_logger_t* logger,
    uint64_t timestamp_mono_ns,
    const char* metric_name,
    double value_ns,
    double threshold_ns,
    const char* severity,
    const char* standard)
{
    if (!logger || !metric_name || !severity) {
        return -1;
    }

    char entry[SWCLOCK_JSONLD_MAX_SIZE];
    char ts_buf[64];
    char escaped_metric[128], escaped_severity[64], escaped_standard[128];

    format_iso8601_ns(timestamp_mono_ns, ts_buf, sizeof(ts_buf));
    json_escape_string(metric_name, escaped_metric, sizeof(escaped_metric));
    json_escape_string(severity, escaped_severity, sizeof(escaped_severity));
    json_escape_string(standard ? standard : "", escaped_standard, sizeof(escaped_standard));

    snprintf(entry, sizeof(entry),
        "{"
        "\"@context\":{\"@vocab\":\"https://swclock.org/vocab#\"},"
        "\"@type\":\"ThresholdAlert\","
        "\"timestamp\":\"%s\","
        "\"timestamp_monotonic_ns\":%llu,"
        "\"event\":{"
        "\"metric\":\"%s\","
        "\"value\":%.6f,"
        "\"threshold\":%.6f,"
        "\"severity\":\"%s\","
        "\"standard\":\"%s\""
        "}"
        "}\n",
        ts_buf,
        (unsigned long long)timestamp_mono_ns,
        escaped_metric,
        value_ns,
        threshold_ns,
        escaped_severity,
        escaped_standard);

    return write_jsonld_entry(logger, entry);
}

int swclock_jsonld_log_system(
    swclock_jsonld_logger_t* logger,
    uint64_t timestamp_mono_ns,
    const char* event_type,
    const char* details_json)
{
    if (!logger || !event_type) {
        return -1;
    }

    char entry[SWCLOCK_JSONLD_MAX_SIZE];
    char ts_buf[64];
    char escaped_type[128];

    format_iso8601_ns(timestamp_mono_ns, ts_buf, sizeof(ts_buf));
    json_escape_string(event_type, escaped_type, sizeof(escaped_type));
    // details_json is already JSON, don't escape it

    snprintf(entry, sizeof(entry),
        "{"
        "\"@context\":{\"@vocab\":\"https://swclock.org/vocab#\"},"
        "\"@type\":\"SystemEvent\","
        "\"timestamp\":\"%s\","
        "\"timestamp_monotonic_ns\":%llu,"
        "\"event\":{"
        "\"event_type\":\"%s\","
        "\"details\":%s"
        "}"
        "}\n",
        ts_buf,
        (unsigned long long)timestamp_mono_ns,
        escaped_type,
        details_json ? details_json : "{}");

    return write_jsonld_entry(logger, entry);
}

int swclock_jsonld_log_metrics(
    swclock_jsonld_logger_t* logger,
    uint64_t timestamp_mono_ns,
    uint32_t sample_count,
    double window_duration_s,
    double mean_te_ns,
    double std_te_ns,
    double min_te_ns,
    double max_te_ns,
    double p95_te_ns,
    double p99_te_ns,
    double mtie_1s_ns,
    double mtie_10s_ns,
    double mtie_30s_ns,
    double mtie_60s_ns,
    double tdev_0_1s_ns,
    double tdev_1s_ns,
    double tdev_10s_ns,
    bool itu_g8260_pass)
{
    if (!logger) {
        return -1;
    }

    char entry[SWCLOCK_JSONLD_MAX_SIZE];
    char ts_buf[64];
    format_iso8601_ns(timestamp_mono_ns, ts_buf, sizeof(ts_buf));

    snprintf(entry, sizeof(entry),
        "{"
        "\"@context\":{\"@vocab\":\"https://swclock.org/vocab#\"},"
        "\"@type\":\"MetricsSnapshot\","
        "\"timestamp\":\"%s\","
        "\"timestamp_monotonic_ns\":%llu,"
        "\"event\":{"
        "\"sample_count\":%u,"
        "\"window_duration_s\":%.2f,"
        "\"te_stats\":{"
        "\"mean_ns\":%.2f,"
        "\"std_ns\":%.2f,"
        "\"min_ns\":%.2f,"
        "\"max_ns\":%.2f,"
        "\"p95_ns\":%.2f,"
        "\"p99_ns\":%.2f"
        "},"
        "\"mtie\":{"
        "\"1s_ns\":%.2f,"
        "\"10s_ns\":%.2f,"
        "\"30s_ns\":%.2f,"
        "\"60s_ns\":%.2f"
        "},"
        "\"tdev\":{"
        "\"0_1s_ns\":%.2f,"
        "\"1s_ns\":%.2f,"
        "\"10s_ns\":%.2f"
        "},"
        "\"compliance\":{"
        "\"itu_g8260_class_c\":{"
        "\"overall_pass\":%s"
        "}"
        "}"
        "}"
        "}\n",
        ts_buf,
        (unsigned long long)timestamp_mono_ns,
        (unsigned)sample_count,
        window_duration_s,
        mean_te_ns,
        std_te_ns,
        min_te_ns,
        max_te_ns,
        p95_te_ns,
        p99_te_ns,
        mtie_1s_ns,
        mtie_10s_ns,
        mtie_30s_ns,
        mtie_60s_ns,
        tdev_0_1s_ns,
        tdev_1s_ns,
        tdev_10s_ns,
        itu_g8260_pass ? "true" : "false");

    return write_jsonld_entry(logger, entry);
}

int swclock_jsonld_log_test(
    swclock_jsonld_logger_t* logger,
    uint64_t timestamp_mono_ns,
    const char* test_name,
    const char* status,
    double duration_ms,
    const char* csv_file,
    const char* metrics_json,
    bool verified,
    double max_error_percent)
{
    if (!logger || !test_name || !status) {
        return -1;
    }

    char entry[SWCLOCK_JSONLD_MAX_SIZE];
    char ts_buf[64];
    char escaped_test[256], escaped_status[64], escaped_csv[512];

    format_iso8601_ns(timestamp_mono_ns, ts_buf, sizeof(ts_buf));
    json_escape_string(test_name, escaped_test, sizeof(escaped_test));
    json_escape_string(status, escaped_status, sizeof(escaped_status));
    json_escape_string(csv_file ? csv_file : "", escaped_csv, sizeof(escaped_csv));
    // metrics_json is already JSON, don't escape it

    snprintf(entry, sizeof(entry),
        "{"
        "\"@context\":{\"@vocab\":\"https://swclock.org/vocab#\"},"
        "\"@type\":\"TestResult\","
        "\"timestamp\":\"%s\","
        "\"timestamp_monotonic_ns\":%llu,"
        "\"event\":{"
        "\"test_name\":\"%s\","
        "\"status\":\"%s\","
        "\"duration_ms\":%.2f,"
        "\"csv_file\":\"%s\","
        "\"metrics\":%s,"
        "\"validation\":{"
        "\"verified\":%s,"
        "\"max_error_percent\":%.2f"
        "}"
        "}"
        "}\n",
        ts_buf,
        (unsigned long long)timestamp_mono_ns,
        escaped_test,
        escaped_status,
        duration_ms,
        escaped_csv,
        metrics_json ? metrics_json : "{}",
        verified ? "true" : "false",
        max_error_percent);

    return write_jsonld_entry(logger, entry);
}

/* ========================================================================
 * Management Functions
 * ======================================================================== */

int swclock_jsonld_rotate(swclock_jsonld_logger_t* logger)
{
    if (!logger) {
        return -1;
    }

    pthread_mutex_lock(&logger->lock);
    int ret = perform_rotation(logger);
    pthread_mutex_unlock(&logger->lock);

    return ret;
}

size_t swclock_jsonld_get_size(swclock_jsonld_logger_t* logger)
{
    if (!logger) {
        return 0;
    }

    pthread_mutex_lock(&logger->lock);
    size_t size = logger->current_size + logger->buffer_pos;
    pthread_mutex_unlock(&logger->lock);

    return size;
}

uint64_t swclock_jsonld_get_count(swclock_jsonld_logger_t* logger)
{
    if (!logger) {
        return 0;
    }

    pthread_mutex_lock(&logger->lock);
    uint64_t count = logger->entry_count;
    pthread_mutex_unlock(&logger->lock);

    return count;
}

/* ========================================================================
 * Helper Functions
 * ======================================================================== */

static int detect_system_context(swclock_system_context_t* ctx)
{
    if (!ctx) {
        return -1;
    }

    /* Get hostname */
    if (gethostname(ctx->hostname, sizeof(ctx->hostname)) != 0) {
        strncpy(ctx->hostname, "unknown", sizeof(ctx->hostname) - 1);
    }

    /* Get OS and kernel info */
    struct utsname uts;
    if (uname(&uts) == 0) {
        strncpy(ctx->os, uts.sysname, sizeof(ctx->os) - 1);
        strncpy(ctx->kernel, uts.release, sizeof(ctx->kernel) - 1);
        strncpy(ctx->arch, uts.machine, sizeof(ctx->arch) - 1);
    } else {
        strncpy(ctx->os, "unknown", sizeof(ctx->os) - 1);
        strncpy(ctx->kernel, "unknown", sizeof(ctx->kernel) - 1);
        strncpy(ctx->arch, "unknown", sizeof(ctx->arch) - 1);
    }

    /* SwClock version - this should ideally come from a build-time constant */
    strncpy(ctx->swclock_version, SWCLOCK_SIF_VERSION, sizeof(ctx->swclock_version) - 1);

    return 0;
}

static int format_iso8601_ns(uint64_t timestamp_ns, char* buf, size_t buflen)
{
    if (!buf || buflen < 64) {
        return -1;
    }

    /* Convert nanoseconds to seconds and fractional part */
    time_t seconds = timestamp_ns / 1000000000ULL;
    uint32_t nanoseconds = timestamp_ns % 1000000000ULL;

    /* Format ISO 8601 with UTC timezone */
    struct tm tm_info;
    if (gmtime_r(&seconds, &tm_info) == NULL) {
        return -1;
    }

    /* Format: 2026-01-13T18:30:45.123456789Z */
    snprintf(buf, buflen, "%04d-%02d-%02dT%02d:%02d:%02d.%09uZ",
             tm_info.tm_year + 1900,
             tm_info.tm_mon + 1,
             tm_info.tm_mday,
             tm_info.tm_hour,
             tm_info.tm_min,
             tm_info.tm_sec,
             nanoseconds);

    return 0;
}

static void json_escape_string(const char* src, char* dst, size_t dst_len)
{
    if (!src || !dst || dst_len == 0) {
        return;
    }

    size_t j = 0;
    for (size_t i = 0; src[i] != '\0' && j < dst_len - 2; i++) {
        switch (src[i]) {
            case '"':
                if (j < dst_len - 3) {
                    dst[j++] = '\\';
                    dst[j++] = '"';
                }
                break;
            case '\\':
                if (j < dst_len - 3) {
                    dst[j++] = '\\';
                    dst[j++] = '\\';
                }
                break;
            case '\n':
                if (j < dst_len - 3) {
                    dst[j++] = '\\';
                    dst[j++] = 'n';
                }
                break;
            case '\r':
                if (j < dst_len - 3) {
                    dst[j++] = '\\';
                    dst[j++] = 'r';
                }
                break;
            case '\t':
                if (j < dst_len - 3) {
                    dst[j++] = '\\';
                    dst[j++] = 't';
                }
                break;
            default:
                if ((unsigned char)src[i] < 32) {
                    /* Escape control characters */
                    if (j < dst_len - 7) {
                        j += snprintf(&dst[j], dst_len - j, "\\u%04x", (unsigned char)src[i]);
                    }
                } else {
                    dst[j++] = src[i];
                }
                break;
        }
    }
    dst[j] = '\0';
}

static int write_jsonld_entry(swclock_jsonld_logger_t* logger, const char* entry)
{
    if (!logger || !entry) {
        return -1;
    }

    size_t entry_len = strlen(entry);
    if (entry_len == 0 || entry_len >= SWCLOCK_JSONLD_MAX_SIZE) {
        return -1;
    }

    pthread_mutex_lock(&logger->lock);

    /* Check if rotation is needed */
    if (logger->rotation.enabled && should_rotate(logger)) {
        if (perform_rotation(logger) != 0) {
            pthread_mutex_unlock(&logger->lock);
            return -1;
        }
    }

    /* If entry won't fit in buffer, flush first */
    if (logger->buffer_pos + entry_len > logger->buffer_size) {
        if (flush_buffer(logger) != 0) {
            pthread_mutex_unlock(&logger->lock);
            return -1;
        }
    }

    /* Add to buffer */
    memcpy(logger->buffer + logger->buffer_pos, entry, entry_len);
    logger->buffer_pos += entry_len;
    logger->entry_count++;

    /* Flush if buffer is getting full (>90%) or every 100 entries */
    if (logger->buffer_pos > (logger->buffer_size * 9 / 10) ||
        (logger->entry_count % 100) == 0) {
        flush_buffer(logger);
    }

    pthread_mutex_unlock(&logger->lock);
    return 0;
}

static int flush_buffer(swclock_jsonld_logger_t* logger)
{
    if (!logger || !logger->fp || logger->buffer_pos == 0) {
        return 0;
    }

    size_t written = fwrite(logger->buffer, 1, logger->buffer_pos, logger->fp);
    if (written != logger->buffer_pos) {
        SWCLOCK_JSONLD_ERROR("swclock_jsonld: Failed to write buffer: %s", strerror(errno));
        return -1;
    }

    /* Update file size */
    logger->current_size += logger->buffer_pos;
    logger->buffer_pos = 0;

    /* Sync to OS buffers (fsync removed to avoid blocking while holding locks) */
    fflush(logger->fp);

    return 0;
}

static int should_rotate(swclock_jsonld_logger_t* logger)
{
    if (!logger || !logger->rotation.enabled) {
        return 0;
    }

    /* Check size threshold */
    if (logger->rotation.max_size_mb > 0) {
        size_t max_bytes = (size_t)logger->rotation.max_size_mb * 1024 * 1024;
        if (logger->current_size + logger->buffer_pos >= max_bytes) {
            return 1;
        }
    }

    /* Check age threshold */
    if (logger->rotation.max_age_hours > 0) {
        time_t now = time(NULL);
        time_t age_seconds = now - logger->created_at;
        time_t max_age_seconds = logger->rotation.max_age_hours * 3600;
        if (age_seconds >= max_age_seconds) {
            return 1;
        }
    }

    return 0;
}

static int perform_rotation(swclock_jsonld_logger_t* logger)
{
    if (!logger) {
        return -1;
    }

    /* Flush and close current file */
    flush_buffer(logger);
    if (logger->fp) {
        fclose(logger->fp);
        logger->fp = NULL;
    }

    /* Rotate existing files: .1 -> .2, .2 -> .3, etc. */
    for (int i = logger->rotation.max_files - 1; i >= 1; i--) {
        char old_path[600], new_path[600];
        snprintf(old_path, sizeof(old_path), "%s.%d", logger->log_path, i);
        snprintf(new_path, sizeof(new_path), "%s.%d", logger->log_path, i + 1);

        /* Check if old file exists */
        if (access(old_path, F_OK) == 0) {
            if (i >= logger->rotation.max_files - 1) {
                /* Delete oldest file */
                unlink(old_path);
            } else {
                /* Rename to next number */
                rename(old_path, new_path);
            }
        }
    }

    /* Rename current log to .1 */
    char rotated_path[600];
    snprintf(rotated_path, sizeof(rotated_path), "%s.1", logger->log_path);
    if (rename(logger->log_path, rotated_path) != 0) {
        SWCLOCK_JSONLD_ERROR("swclock_jsonld: Failed to rotate log file: %s", strerror(errno));
        /* Try to continue by opening new file anyway */
    }

    /* Compress rotated file if enabled */
    if (logger->rotation.compress) {
        compress_file(rotated_path);
    }

    /* Open new log file */
    logger->fp = fopen(logger->log_path, "a");
    if (!logger->fp) {
        SWCLOCK_JSONLD_ERROR("swclock_jsonld: Failed to open new log file: %s", strerror(errno));
        return -1;
    }

    /* Reset counters */
    logger->created_at = time(NULL);
    logger->current_size = 0;

    return 0;
}

static int compress_file(const char* src_path)
{
    if (!src_path) {
        return -1;
    }

    char dst_path[600];
    snprintf(dst_path, sizeof(dst_path), "%s.gz", src_path);

    /* Open source file */
    FILE* src = fopen(src_path, "rb");
    if (!src) {
        return -1;
    }

    /* Open gzip destination */
    gzFile dst = gzopen(dst_path, "wb9"); /* Best compression */
    if (!dst) {
        fclose(src);
        return -1;
    }

    /* Compress in chunks */
    char buf[8192];
    size_t nread;
    while ((nread = fread(buf, 1, sizeof(buf), src)) > 0) {
        if (gzwrite(dst, buf, nread) != (int)nread) {
            fclose(src);
            gzclose(dst);
            unlink(dst_path);
            return -1;
        }
    }

    fclose(src);
    gzclose(dst);

    /* Delete original file */
    unlink(src_path);

    return 0;
}
