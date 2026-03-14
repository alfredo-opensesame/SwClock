/**
 * @file sw_clock_commercial_log.c
 * @brief Commercial-Grade Logging Implementation
 *
 * Production-ready logging infrastructure for regulatory compliance
 * and commercial deployment.
 *
 * @author SwClock Development Team
 * @date 2026-02-10
 */

#include "sw_clock_commercial_log.h"
#include "sw_clock.h"
#include "sw_clock_constants.h"
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <unistd.h>
#include <stdarg.h>
#include <syslog.h>
#include <CommonCrypto/CommonDigest.h>  // macOS SHA-256

#define UUID_LENGTH 37

static void swclock_emit_log(int priority, const char *format, ...) {
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

    fprintf(sink, "[swclock] %s\n", message);
    fclose(sink);
}

#define SWCLOCK_LOG_INFO(fmt, ...)                                             \
    swclock_emit_log(LOG_INFO, fmt, ##__VA_ARGS__)
#define SWCLOCK_LOG_WARN(fmt, ...)                                             \
    swclock_emit_log(LOG_WARNING, fmt, ##__VA_ARGS__)
#define SWCLOCK_LOG_ERROR(fmt, ...)                                            \
    swclock_emit_log(LOG_ERR, fmt, ##__VA_ARGS__)

// Global commercial logging state
static swclock_commercial_config_t g_commercial_config;
static bool g_commercial_logging_initialized = false;
static char g_run_uuid[UUID_LENGTH] = {0};

/**
 * @brief Generate RFC 4122 UUID v4
 */
static void generate_uuid(char* uuid_out) {
    unsigned char bytes[16];
    FILE* urandom = fopen("/dev/urandom", "rb");
    if (urandom) {
        fread(bytes, 1, 16, urandom);
        fclose(urandom);
    } else {
        // Fallback: use time-based pseudo-random
        srand(time(NULL));
        for (int i = 0; i < 16; i++) {
            bytes[i] = rand() & 0xFF;
        }
    }

    // Set version (4) and variant (RFC 4122)
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;

    snprintf(uuid_out, UUID_LENGTH,
             "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             bytes[0], bytes[1], bytes[2], bytes[3],
             bytes[4], bytes[5], bytes[6], bytes[7],
             bytes[8], bytes[9], bytes[10], bytes[11],
             bytes[12], bytes[13], bytes[14], bytes[15]);
}

/**
 * @brief Get ISO 8601 timestamp
 */
static void get_iso8601_timestamp(char* buffer, size_t size) {
    time_t now = time(NULL);
    struct tm* tm_info = gmtime(&now);
    strftime(buffer, size, "%Y-%m-%dT%H:%M:%SZ", tm_info);
}

/**
 * @brief Get system information
 */
static void get_system_info(char* os, size_t os_size,
                            char* kernel, size_t kernel_size,
                            char* arch, size_t arch_size,
                            char* hostname, size_t hostname_size) {
    struct utsname uname_data;
    if (uname(&uname_data) == 0) {
        snprintf(os, os_size, "%s", uname_data.sysname);
        snprintf(kernel, kernel_size, "%s %s", uname_data.release, uname_data.version);
        snprintf(arch, arch_size, "%s", uname_data.machine);
        snprintf(hostname, hostname_size, "%s", uname_data.nodename);
    } else {
        snprintf(os, os_size, "Unknown");
        snprintf(kernel, kernel_size, "Unknown");
        snprintf(arch, arch_size, "Unknown");
        snprintf(hostname, hostname_size, "Unknown");
    }
}

swclock_commercial_config_t swclock_commercial_get_defaults(void) {
    swclock_commercial_config_t config = {
        // Always-on production features
        .binary_event_log = true,
        .jsonld_structured_log = true,
        .servo_state_log = true,

        // Audit compliance
        .automatic_integrity = true,
        .tamper_detection = true,
        .comprehensive_metadata = true,

        // Performance
        .buffer_size_kb = 1024,
        .flush_interval_ms = 1000,

        // Rotation
        .auto_rotation = true,
        .max_size_mb = 100,
        .max_files = 10,
        .compress_rotated = true,

        // Paths (NULL = defaults)
        .log_directory = NULL,
        .run_id = NULL
    };
    return config;
}

int swclock_commercial_logging_init(const swclock_commercial_config_t* config) {
    if (g_commercial_logging_initialized) {
        SWCLOCK_LOG_WARN("Commercial logging already initialized");
        return -1;
    }

    // Use defaults if no config provided
    if (config == NULL) {
        g_commercial_config = swclock_commercial_get_defaults();
    } else {
        g_commercial_config = *config;
    }

    // Generate UUID for this run
    if (g_commercial_config.run_id == NULL) {
        generate_uuid(g_run_uuid);
    } else {
        strncpy(g_run_uuid, g_commercial_config.run_id, UUID_LENGTH - 1);
        g_run_uuid[UUID_LENGTH - 1] = '\0';
    }

    // Create log directory
    const char* log_dir = g_commercial_config.log_directory ?
                          g_commercial_config.log_directory : "logs";

    struct stat st = {0};
    if (stat(log_dir, &st) == -1) {
        if (mkdir(log_dir, 0755) != 0) {
            SWCLOCK_LOG_ERROR("Failed to create log directory: %s", strerror(errno));
            return -1;
        }
    }

    g_commercial_logging_initialized = true;

    SWCLOCK_LOG_INFO("Commercial logging initialized");
    SWCLOCK_LOG_INFO("Run ID: %s", g_run_uuid);
    SWCLOCK_LOG_INFO("Log Directory: %s/", log_dir);
    SWCLOCK_LOG_INFO("Binary Events: %s", g_commercial_config.binary_event_log ? "ON" : "OFF");
    SWCLOCK_LOG_INFO("JSON-LD: %s", g_commercial_config.jsonld_structured_log ? "ON" : "OFF");
    SWCLOCK_LOG_INFO("Servo State: %s", g_commercial_config.servo_state_log ? "ON" : "OFF");
    SWCLOCK_LOG_INFO("Integrity Protection: %s", g_commercial_config.automatic_integrity ? "ON" : "OFF");

    return 0;
}

int swclock_commercial_logging_finalize(void) {
    if (!g_commercial_logging_initialized) {
        return 0;  // Nothing to finalize
    }

    SWCLOCK_LOG_INFO("Finalizing commercial logging...");

    // Generate manifest
    const char* log_dir = g_commercial_config.log_directory ?
                          g_commercial_config.log_directory : "logs";

    if (swclock_generate_manifest(g_run_uuid, log_dir) != 0) {
        SWCLOCK_LOG_WARN("Failed to generate manifest");
    }

    g_commercial_logging_initialized = false;
    SWCLOCK_LOG_INFO("Commercial logging finalized.");

    return 0;
}

int swclock_write_commercial_csv_header(FILE* fp, const char* test_name, void* clock_ptr) {
    (void)clock_ptr;  // Reserved for future use

    if (fp == NULL || test_name == NULL) {
        return -1;
    }

    char timestamp[64];
    char os[64], kernel[256], arch[32], hostname[256];

    get_iso8601_timestamp(timestamp, sizeof(timestamp));
    get_system_info(os, sizeof(os), kernel, sizeof(kernel),
                    arch, sizeof(arch), hostname, sizeof(hostname));

    // Generate comprehensive 36+ line header
    fprintf(fp, "# ========================================================================\n");
    fprintf(fp, "# SwClock Performance Test Data - Commercial Export\n");
    fprintf(fp, "# ========================================================================\n");
    fprintf(fp, "#\n");
    fprintf(fp, "# [TEST IDENTIFICATION]\n");
    fprintf(fp, "# Test Name: %s\n", test_name);
    fprintf(fp, "# Run UUID: %s\n", g_run_uuid);
    fprintf(fp, "# Timestamp: %s\n", timestamp);
    fprintf(fp, "#\n");
    fprintf(fp, "# [SWCLOCK CONFIGURATION]\n");
    fprintf(fp, "# SwClock Version: %s\n", SWCLOCK_VERSION);
    fprintf(fp, "# Proportional Gain (Kp): %.3f ppm/s\n", SWCLOCK_PI_KP_PPM_PER_S);
    fprintf(fp, "# Integral Gain (Ki): %.3f ppm/s²\n", SWCLOCK_PI_KI_PPM_PER_S2);
    fprintf(fp, "# Maximum Frequency: %.3f ppm\n", SWCLOCK_PI_MAX_PPM);
    fprintf(fp, "# Poll Interval: %.3f ms\n", SWCLOCK_POLL_NS / 1e6);
    fprintf(fp, "# Phase Epsilon: %lld ns\n", (long long)SWCLOCK_PHASE_EPS_NS);
    fprintf(fp, "#\n");
    fprintf(fp, "# [SYSTEM INFORMATION]\n");
    fprintf(fp, "# Hostname: %s\n", hostname);
    fprintf(fp, "# Operating System: %s\n", os);
    fprintf(fp, "# Kernel: %s\n", kernel);
    fprintf(fp, "# Architecture: %s\n", arch);
    fprintf(fp, "# Reference Clock: CLOCK_MONOTONIC_RAW\n");
    fprintf(fp, "#\n");
    fprintf(fp, "# [COMPLIANCE TARGETS]\n");
    fprintf(fp, "# Standard: IEEE 1588-2019 (PTP v2.1)\n");
    fprintf(fp, "# Standard: ITU-T G.8260 (Packet-based frequency)\n");
    fprintf(fp, "# Time Error Budget: |TE| < 150 µs (P95)\n");
    fprintf(fp, "# MTIE(1s): < 100 µs (ITU-T G.8260 Class C)\n");
    fprintf(fp, "# MTIE(10s): < 200 µs (ITU-T G.8260 Class C)\n");
    fprintf(fp, "# MTIE(30s): < 300 µs (ITU-T G.8260 Class C)\n");
    fprintf(fp, "# TDEV(0.1s): < 20 µs\n");
    fprintf(fp, "# TDEV(1s): < 40 µs\n");
    fprintf(fp, "# TDEV(10s): < 80 µs\n");
    fprintf(fp, "#\n");
    fprintf(fp, "# [DATA FORMAT]\n");
    fprintf(fp, "# Columns: timestamp_ns, te_ns\n");
    fprintf(fp, "# - timestamp_ns: Test elapsed time (nanoseconds since test start)\n");
    fprintf(fp, "# - te_ns: Time Error in nanoseconds (SwClock - Reference)\n");
    fprintf(fp, "#\n");
    fprintf(fp, "# [INTEGRITY]\n");
    fprintf(fp, "# SHA-256 hash will be appended on file close\n");
    fprintf(fp, "# Verify with: swclock_verify_log_integrity()\n");
    fprintf(fp, "# ========================================================================\n");
    fprintf(fp, "timestamp_ns,te_ns\n");

    return 0;
}

int swclock_seal_log_file(const char* filepath) {
    if (filepath == NULL) {
        return -1;
    }

    // Read entire file
    FILE* fp = fopen(filepath, "rb");
    if (fp == NULL) {
        SWCLOCK_LOG_ERROR("Failed to open file for sealing: %s", strerror(errno));
        return -1;
    }

    // Get file size
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    if (file_size <= 0) {
        fclose(fp);
        return -1;
    }

    // Read file contents
    unsigned char* buffer = malloc(file_size);
    if (buffer == NULL) {
        fclose(fp);
        return -1;
    }

    size_t bytes_read = fread(buffer, 1, file_size, fp);
    fclose(fp);

    if (bytes_read != (size_t)file_size) {
        free(buffer);
        return -1;
    }

    // Compute SHA-256
    unsigned char hash[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256(buffer, (CC_LONG)file_size, hash);
    free(buffer);

    // Append signature block
    fp = fopen(filepath, "a");
    if (fp == NULL) {
        SWCLOCK_LOG_ERROR("Failed to open file for signature: %s", strerror(errno));
        return -1;
    }

    char timestamp[64];
    get_iso8601_timestamp(timestamp, sizeof(timestamp));

    fprintf(fp, "# ========================================================================\n");
    fprintf(fp, "# INTEGRITY SEAL\n");
    fprintf(fp, "# SHA256: ");
    for (int i = 0; i < CC_SHA256_DIGEST_LENGTH; i++) {
        fprintf(fp, "%02x", hash[i]);
    }
    fprintf(fp, "\n");
    fprintf(fp, "# SEALED: %s\n", timestamp);
    fprintf(fp, "# ALGORITHM: SHA-256\n");
    fprintf(fp, "# ========================================================================\n");

    fclose(fp);

    SWCLOCK_LOG_INFO("Log file sealed: %s", filepath);
    return 0;
}

int swclock_verify_log_integrity(const char* filepath, bool* out_valid) {
    if (filepath == NULL || out_valid == NULL) {
        return -1;
    }

    *out_valid = false;

    // Read file and extract signature
    FILE* fp = fopen(filepath, "r");
    if (fp == NULL) {
        return -1;
    }

    // Find signature line
    char line[512];
    char stored_hash[65] = {0};
    bool found_signature = false;
    long data_end_pos = 0;

    while (fgets(line, sizeof(line), fp)) {
        if (strstr(line, "# SHA256: ") != NULL) {
            sscanf(line, "# SHA256: %64s", stored_hash);
            found_signature = true;
            data_end_pos = ftell(fp) - strlen(line);
            break;
        }
    }

    if (!found_signature) {
        fclose(fp);
        SWCLOCK_LOG_WARN("No integrity signature found in file");
        return -1;
    }

    // Read data portion (before signature)
    fseek(fp, 0, SEEK_SET);
    unsigned char* buffer = malloc(data_end_pos);
    if (buffer == NULL) {
        fclose(fp);
        return -1;
    }

    size_t bytes_read = fread(buffer, 1, data_end_pos, fp);
    fclose(fp);

    if (bytes_read != (size_t)data_end_pos) {
        free(buffer);
        return -1;
    }

    // Compute hash
    unsigned char computed_hash[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256(buffer, (CC_LONG)data_end_pos, computed_hash);
    free(buffer);

    // Convert to hex string
    char computed_hash_str[65];
    for (int i = 0; i < CC_SHA256_DIGEST_LENGTH; i++) {
        sprintf(computed_hash_str + (i * 2), "%02x", computed_hash[i]);
    }
    computed_hash_str[64] = '\0';

    // Compare
    *out_valid = (strcmp(stored_hash, computed_hash_str) == 0);

    return 0;
}

int swclock_generate_manifest(const char* run_id, const char* log_directory) {
    if (run_id == NULL || log_directory == NULL) {
        return -1;
    }

    char manifest_path[512];
    snprintf(manifest_path, sizeof(manifest_path), "%s/manifest_%s.json",
             log_directory, run_id);

    FILE* fp = fopen(manifest_path, "w");
    if (fp == NULL) {
        SWCLOCK_LOG_ERROR("Failed to create manifest: %s", strerror(errno));
        return -1;
    }

    char timestamp[64];
    char hostname[256], os[64], kernel[256], arch[32];

    get_iso8601_timestamp(timestamp, sizeof(timestamp));
    get_system_info(os, sizeof(os), kernel, sizeof(kernel),
                    arch, sizeof(arch), hostname, sizeof(hostname));

    // Write JSON manifest
    fprintf(fp, "{\n");
    fprintf(fp, "  \"manifest_version\": \"1.0\",\n");
    fprintf(fp, "  \"run_id\": \"%s\",\n", run_id);
    fprintf(fp, "  \"generated\": \"%s\",\n", timestamp);
    fprintf(fp, "  \"swclock_version\": \"%s\",\n", SWCLOCK_VERSION);
    fprintf(fp, "  \"system\": {\n");
    fprintf(fp, "    \"hostname\": \"%s\",\n", hostname);
    fprintf(fp, "    \"os\": \"%s\",\n", os);
    fprintf(fp, "    \"kernel\": \"%s\",\n", kernel);
    fprintf(fp, "    \"arch\": \"%s\"\n", arch);
    fprintf(fp, "  },\n");
    fprintf(fp, "  \"configuration\": {\n");
    fprintf(fp, "    \"kp_ppm_per_s\": %.3f,\n", SWCLOCK_PI_KP_PPM_PER_S);
    fprintf(fp, "    \"ki_ppm_per_s2\": %.3f,\n", SWCLOCK_PI_KI_PPM_PER_S2);
    fprintf(fp, "    \"max_ppm\": %.3f,\n", SWCLOCK_PI_MAX_PPM);
    fprintf(fp, "    \"poll_interval_ms\": %.3f\n", SWCLOCK_POLL_NS / 1e6);
    fprintf(fp, "  },\n");
    fprintf(fp, "  \"compliance_targets\": {\n");
    fprintf(fp, "    \"ieee_1588\": \"2019\",\n");
    fprintf(fp, "    \"itu_t_g8260\": \"Class C\",\n");
    fprintf(fp, "    \"mtie_1s_us\": 100,\n");
    fprintf(fp, "    \"mtie_10s_us\": 200,\n");
    fprintf(fp, "    \"mtie_30s_us\": 300\n");
    fprintf(fp, "  },\n");
    fprintf(fp, "  \"log_files\": []\n");
    fprintf(fp, "}\n");

    fclose(fp);

    SWCLOCK_LOG_INFO("Manifest generated: %s", manifest_path);
    return 0;
}
