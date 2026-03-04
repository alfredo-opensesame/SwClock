//
// sw_clock.c (v2.0 - slewed phase correction with PI + background poll thread)
//
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <inttypes.h> // for PRId64
#include <sys/time.h>

#include "sw_clock.h"
#include "swclock_jsonld.h"
#include "sw_clock_commercial_log.h"

// Debug instrumentation (enable with -DSWCLOCK_DEBUG)
#ifdef SWCLOCK_DEBUG
#define DEBUG_LOG(fmt, ...) \
    do { \
        struct timespec _ts; \
        clock_gettime(CLOCK_REALTIME, &_ts); \
        fprintf(stderr, "[%ld.%06ld] SwClock[%p] " fmt "\n", \
                _ts.tv_sec, _ts.tv_nsec / 1000, (void*)c, ##__VA_ARGS__); \
        fflush(stderr); \
    } while(0)
#else
#define DEBUG_LOG(fmt, ...) do {} while(0)
#endif



// ================= Helpers =================
void swclock_log(SwClock* c);

static inline double scaledppm_to_ppm(long scaled) {
    return ((double)scaled) / 65536.0;
}

// ================= Core state =================

struct SwClock {
    pthread_rwlock_t lock;  // Changed from mutex to rwlock (poll=writer, gettime=reader)

    // Reference epoch from hardware raw time (updated only by poll thread)
    struct timespec ref_mono_raw;

    // Software clock bases at reference (updated only by poll thread) 
    int64_t base_rt_ns;        // REALTIME
    int64_t base_mono_ns;      // disciplined synthetic timebase
    
    // Cached total factor for gettime extrapolation
    double cached_total_factor;

    // Base frequency bias set by ADJ_FREQUENCY (scaled-ppm)
    long    freq_scaled_ppm;

    // PI controller state (produces an additional freq correction in ppm)
    double  pi_freq_ppm;           // output
    double  pi_int_error_s;        // integral of error (seconds)
    bool    pi_servo_enabled;      // whether PI servo is active

    // Outstanding phase error to slew out (nanoseconds). Sign indicates direction.
    long long remaining_phase_ns;

    // Watchdog state for detecting stuck servo
    long long last_remaining_phase_ns;
    int stuck_poll_count;
    struct timespec last_poll_time;

    // Error tracking for maxerror/esterror computation
    double    max_observed_phase_error_s;    // maximum phase error seen (seconds)
    double    accumulated_error_variance;    // accumulated error variance for estimation
    long long error_samples_count;          // number of error samples collected

    // Timex-ish fields
    int     status;   // status word
    long    maxerror; // maximum error (nanoseconds)
    long    esterror; // estimated error (nanoseconds)
    long    constant; // constant (nanoseconds)
    long    tick;     // tick (nanoseconds)
    int     tai;      // TAI offset (seconds)

    // Background poll thread
    pthread_t poll_thread;
    bool      poll_thread_running;
    bool      stop_flag;

    // Logging support
    FILE* log_fp;         // CSV file handle
    bool  is_logging;     // true if logging is active
    bool  servo_log_enabled;  // true if SWCLOCK_SERVO_LOG was set at creation
    
    // Event logging support (Priority 1 Recommendation 2)
    FILE* event_log_fp;             // Binary event log file
    bool  event_logging_enabled;    // Event logging active flag
    swclock_ringbuf_t event_ringbuf; // Lock-free event buffer
    pthread_t event_logger_thread;  // Background logger thread
    bool event_logger_running;      // Logger thread status
    uint64_t event_sequence;        // Event sequence number
    
    // Real-time monitoring (Priority 2 Recommendation 7)
    swclock_monitor_t* monitor;     // Monitoring context (NULL if disabled)
    
    // JSON-LD structured logging (Priority 2 Recommendation 10)
    swclock_jsonld_logger_t* jsonld_logger;  // JSON-LD logger (NULL if disabled)
    bool monitoring_enabled;         // Monitoring active flag
};

// Forward declaration
static void* swclock_poll_thread_main(void* arg);

// Compute total rate factor from base freq + PI freq correction (in ppm)
static inline double total_factor(const struct SwClock* c) {
    double base_ppm = scaledppm_to_ppm(c->freq_scaled_ppm);
    double total_ppm = base_ppm + c->pi_freq_ppm;
    return 1.0 + total_ppm / 1.0e6;
}

// Advance time bases to now using current total factor and update remaining phase bookkeeping.
static void swclock_rebase_now_and_update(SwClock* c) {
    struct timespec now_raw;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &now_raw) != 0) return;

    int64_t elapsed_raw_ns = ts_to_ns(&now_raw) - ts_to_ns(&c->ref_mono_raw);
    if (elapsed_raw_ns < 0) elapsed_raw_ns = 0;

    double factor = total_factor(c);
    int64_t adj_elapsed_ns = (int64_t)((double)elapsed_raw_ns * factor);
    
    DEBUG_LOG("rebase: elapsed_raw=%lld ns, factor=%.9f, adj_elapsed=%lld ns",
              (long long)elapsed_raw_ns, factor, (long long)adj_elapsed_ns);

    // Sanity check: detect if factor calculation produced invalid results
    if (factor < 0.999 || factor > 1.001) {
        DEBUG_LOG("WARNING: total_factor out of bounds: %.9f (base_ppm=%.3f, pi_freq_ppm=%.3f)",
                  factor, scaledppm_to_ppm(c->freq_scaled_ppm), c->pi_freq_ppm);
    }

    // Update bases with total factor
    c->base_rt_ns   += adj_elapsed_ns;
    c->base_mono_ns += adj_elapsed_ns;

    // Bookkeeping: determine how much of that advancement was due to PI frequency
    double    base_factor      = scaledppm_to_factor(c->freq_scaled_ppm);
    double    delta_factor     = factor - base_factor;
    long long applied_phase_ns = (long long)((double)elapsed_raw_ns * delta_factor);
    
    long long before_remaining = c->remaining_phase_ns;

    // Reduce remaining phase by what PI rate has effectively corrected
    if (c->remaining_phase_ns != 0) {
        if (llabs(c->remaining_phase_ns) <= llabs(applied_phase_ns)) {
            c->remaining_phase_ns = 0;
        } else {
            // Subtract the applied correction from remaining, regardless of sign
            // The PI servo generates corrections opposite to the error, so this reduces the magnitude
            c->remaining_phase_ns -= applied_phase_ns;
        }
        
        if (before_remaining != 0) {
            DEBUG_LOG("rebase: remaining_phase: %lld -> %lld ns (applied_phase=%lld ns)",
                      before_remaining, c->remaining_phase_ns, applied_phase_ns);
        }
    }

    c->ref_mono_raw = now_raw;
    
    // Cache total factor for gettime extrapolation
    c->cached_total_factor = factor;
}


void swclock_disable_pi_servo(SwClock* c)
{
    if (!c) return;
    pthread_rwlock_wrlock(&c->lock);
    c->pi_servo_enabled = false;
    pthread_rwlock_unlock(&c->lock);
    
    // Log PI disable event
    swclock_log_event(c, SWCLOCK_EVENT_PI_DISABLE, NULL, 0);
}

long long swclock_get_remaining_phase_ns(SwClock* c) {
    if (!c) return 0;
    pthread_rwlock_wrlock(&c->lock);
    long long remaining = c->remaining_phase_ns;
    pthread_rwlock_unlock(&c->lock);
    return remaining;
}

// One PI control step. dt_s is the elapsed RAW time since last poll in seconds.
static void swclock_pi_step(SwClock* c, double dt_s) {

    if (c->pi_servo_enabled == false) {
        // PI servo is disabled; do nothing
        return;
    }

    // Error is the remaining phase (seconds). Positive error => need faster time (positive ppm).
    double err_s = (double)c->remaining_phase_ns / 1e9;

    // Integrator
    c->pi_int_error_s += err_s * dt_s;

    // PI output in ppm
    double u_ppm = (SWCLOCK_PI_KP_PPM_PER_S * err_s) + (SWCLOCK_PI_KI_PPM_PER_S2 * c->pi_int_error_s);

    // For active slewing (remaining_phase_ns != 0), ensure minimum slew rate
    // to avoid excessive settling time for very small offsets
    // Only apply if PI output is below minimum AND offset is small enough
    if (c->remaining_phase_ns != 0 && fabs(err_s) < 0.01) {  // < 10ms offset
        const double MIN_SLEW_PPM = 100.0;  // Minimum slew rate for ADJ_OFFSET
        if (fabs(u_ppm) < MIN_SLEW_PPM) {
            u_ppm = (c->remaining_phase_ns > 0) ? MIN_SLEW_PPM : -MIN_SLEW_PPM;
        }
    }

    // Clamp
    bool clamped = false;
    if (u_ppm > SWCLOCK_PI_MAX_PPM) {
        u_ppm = SWCLOCK_PI_MAX_PPM;
        clamped = true;
    }
    if (u_ppm < -SWCLOCK_PI_MAX_PPM) {
        u_ppm = -SWCLOCK_PI_MAX_PPM;
        clamped = true;
    }

    c->pi_freq_ppm = u_ppm;
    
    // Log frequency clamp event if clamped
    if (clamped) {
        swclock_event_frequency_clamp_payload_t clamp_payload = {
            .requested_ppm = (SWCLOCK_PI_KP_PPM_PER_S * err_s) + (SWCLOCK_PI_KI_PPM_PER_S2 * c->pi_int_error_s),
            .clamped_ppm = u_ppm,
            .max_ppm = SWCLOCK_PI_MAX_PPM
        };
        swclock_log_event(c, SWCLOCK_EVENT_FREQUENCY_CLAMP, &clamp_payload, sizeof(clamp_payload));
    }
    
    // Log PI step event
    swclock_event_pi_step_payload_t pi_payload = {
        .pi_freq_ppm = c->pi_freq_ppm,
        .pi_int_error_s = c->pi_int_error_s,
        .remaining_phase_ns = c->remaining_phase_ns,
        .servo_enabled = c->pi_servo_enabled ? 1 : 0
    };
    swclock_log_event(c, SWCLOCK_EVENT_PI_STEP, &pi_payload, sizeof(pi_payload));
    
    // JSON-LD logging
    if (c->jsonld_logger) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        uint64_t timestamp_ns = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
        swclock_jsonld_log_pi_update(c->jsonld_logger, timestamp_ns,
            SWCLOCK_PI_KP_PPM_PER_S, SWCLOCK_PI_KI_PPM_PER_S2,
            err_s, c->pi_freq_ppm, c->pi_int_error_s);
    }

    // Anti-windup: if close enough, zero everything
    if (llabs(c->remaining_phase_ns) <= SWCLOCK_PHASE_EPS_NS) {
        c->remaining_phase_ns = 0;
        c->pi_int_error_s     = 0.0;
        c->pi_freq_ppm        = 0.0;
        c->max_observed_phase_error_s *= 0.0; 
    }
}

// Update error estimates based on current PI servo state
static void swclock_update_error_estimates(SwClock* c) {
    // Current phase error in seconds
    double current_phase_error_s = fabs((double)c->remaining_phase_ns / 1e9);
    
    // Update maximum observed phase error
    if (current_phase_error_s > c->max_observed_phase_error_s) {
        c->max_observed_phase_error_s = current_phase_error_s;
    }
    
    // Update accumulated error variance (simple moving average approach)
    c->error_samples_count++;
    double alpha = 1.0 / (double)c->error_samples_count;
    if (c->error_samples_count > 100) alpha = 0.01; // Limit history to ~100 samples
    
    double error_contribution = current_phase_error_s * current_phase_error_s;
    c->accumulated_error_variance = (1.0 - alpha) * c->accumulated_error_variance + alpha * error_contribution;
    
    // Calculate maxerror: maximum observed + integral error contribution
    // Convert to microseconds (Linux convention)
    double max_error_s = c->max_observed_phase_error_s + fabs(c->pi_int_error_s);
    c->maxerror = (long)(max_error_s * 1e6);
    
    // Calculate esterror: RMS of accumulated variance + PI frequency contribution
    // Higher PI frequency corrections indicate higher uncertainty
    double estimated_error_s = sqrt(c->accumulated_error_variance) + 0.1 * fabs(c->pi_freq_ppm) / 1e6;
    c->esterror = (long)(estimated_error_s * 1e6);
    
    // Ensure reasonable bounds (max 1 second error estimates)
    if (c->maxerror > 1000000) c->maxerror = 1000000;
    if (c->esterror > 1000000) c->esterror = 1000000;
}

// Public poll: advance to now, then do one PI update based on elapsed dt.
void swclock_poll(SwClock* c) {
    if (!c) return;
    
    struct timespec poll_start;
    clock_gettime(CLOCK_REALTIME, &poll_start);
    
    // Acquire write lock (exclusive) - no gettime() calls can proceed while poll updates
    pthread_rwlock_wrlock(&c->lock);

    struct timespec before = c->ref_mono_raw;
    
    swclock_rebase_now_and_update(c);

    int64_t dt_ns = ts_to_ns(&c->ref_mono_raw) - ts_to_ns(&before);
    double dt_s = (dt_ns > 0) ? (double)dt_ns / 1e9 : (double)SWCLOCK_POLL_NS / 1e9;

    if (c->pi_servo_enabled) {
        swclock_pi_step(c, dt_s);
    }
    
    // Watchdog: detect stuck servo
    if (c->remaining_phase_ns != 0 && c->remaining_phase_ns == c->last_remaining_phase_ns) {
        c->stuck_poll_count++;
        if (c->stuck_poll_count > 20) {
            DEBUG_LOG("WARNING: Servo stuck! remaining_phase_ns=%lld for %d polls, pi_int_error_s=%.9f, pi_freq_ppm=%.3f",
                      c->remaining_phase_ns, c->stuck_poll_count, c->pi_int_error_s, c->pi_freq_ppm);
        }
    } else {
        c->stuck_poll_count = 0;
    }
    c->last_remaining_phase_ns = c->remaining_phase_ns;
    c->last_poll_time = poll_start;
    
    // Bounds checking
    if (llabs(c->remaining_phase_ns) > 1000000000LL) {  // >1 second
        DEBUG_LOG("WARNING: remaining_phase_ns out of bounds: %lld ns", c->remaining_phase_ns);
    }
    if (fabs(c->pi_int_error_s) > 1.0) {  // >1 second integral
        DEBUG_LOG("WARNING: pi_int_error_s excessive: %.9f s", c->pi_int_error_s);
    }
    if (fabs(c->pi_freq_ppm) > (double)SWCLOCK_PI_MAX_PPM + 50.0) {
        DEBUG_LOG("WARNING: pi_freq_ppm beyond clamp: %.3f ppm", c->pi_freq_ppm);
    }

    // Update error estimates based on current state
    swclock_update_error_estimates(c);

    pthread_rwlock_unlock(&c->lock);
}

// ================= Public API =================

SwClock* swclock_create(void) {
    SwClock* c = (SwClock*)calloc(1, sizeof(SwClock));
    if (!c) return NULL;

    pthread_rwlock_init(&c->lock, NULL);

    clock_gettime(CLOCK_MONOTONIC_RAW, &c->ref_mono_raw);

    struct timespec sys_rt = {0}, sys_mono_raw = {0};
    clock_gettime(CLOCK_REALTIME, &sys_rt);
    clock_gettime(CLOCK_MONOTONIC_RAW, &sys_mono_raw);

    c->base_rt_ns   = ts_to_ns(&sys_rt);
    c->base_mono_ns = ts_to_ns(&sys_mono_raw);

    c->freq_scaled_ppm    = 0;
    c->pi_freq_ppm        = 0.0;
    c->pi_int_error_s     = 0.0;
    c->pi_servo_enabled   = true;
    c->remaining_phase_ns = 0;
    c->cached_total_factor = 1.0;
    
    // Initialize watchdog
    c->last_remaining_phase_ns = 0;
    c->stuck_poll_count = 0;
    c->last_poll_time = c->ref_mono_raw;

    // Initialize error tracking
    c->max_observed_phase_error_s = 0.0;
    c->accumulated_error_variance = 0.0;
    c->error_samples_count       = 0;

    c->status   = 0; 
    c->maxerror = 0; 
    c->esterror = 0; 
    c->constant = 0; 
    c->tick     = 0; 
    c->tai      = 0;

    c->stop_flag           = false;
    c->poll_thread_running = true;
    
    // COMMERCIAL DEPLOYMENT: Enable servo logging by default (no environment variable required)
    // For production, comprehensive logging is always enabled unless explicitly disabled
    const char* disable_servo_log = getenv("SWCLOCK_DISABLE_SERVO_LOG");
    c->servo_log_enabled = (disable_servo_log == NULL || atoi(disable_servo_log) == 0);
    
    // Initialize event logging fields
    c->event_log_fp = NULL;
    c->event_logging_enabled = false;
    c->event_logger_running = false;
    c->event_sequence = 0;
    swclock_ringbuf_init(&c->event_ringbuf);
    
    // Initialize monitoring fields (Rec 7)
    c->monitor = NULL;
    c->monitoring_enabled = false;
    
    // COMMERCIAL DEPLOYMENT: Enable JSON-LD structured logging by default
    // This provides audit-compliant logging for regulatory environments
    // Can be disabled with SWCLOCK_DISABLE_JSONLD=1 for embedded systems
    c->jsonld_logger = NULL;
    const char* disable_jsonld = getenv("SWCLOCK_DISABLE_JSONLD");
    if (disable_jsonld == NULL || atoi(disable_jsonld) == 0) {
        swclock_log_rotation_t rotation = {
            .enabled = true,
            .max_size_mb = 100,
            .max_age_hours = 168,  // 7 days
            .max_files = 10,
            .compress = true
        };
        c->jsonld_logger = swclock_jsonld_init("logs/swclock.jsonl", &rotation, NULL);
        if (c->jsonld_logger) {
            // Log system startup event
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            uint64_t timestamp_ns = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
            char details[256];
            snprintf(details, sizeof(details), "{\"version\":\"2.0.0\",\"build\":\"commercial\"}");
            swclock_jsonld_log_system(c->jsonld_logger, timestamp_ns, "swclock_start", details);
        }
    }

    if (pthread_create(&c->poll_thread, NULL, swclock_poll_thread_main, c) != 0) {
        c->poll_thread_running = false;
    }

    return c;
}

void swclock_destroy(SwClock* c) {
    if (!c) return;

    if (c->poll_thread_running) {
        // First, signal the thread to stop
        pthread_rwlock_wrlock(&c->lock);
        c->stop_flag = true;
        pthread_rwlock_unlock(&c->lock);
        
        // Wait for thread to exit
        pthread_join(c->poll_thread, NULL);
        
        // Now safely close the logs
        swclock_close_log(c);
        swclock_stop_event_log(c);
        
        // Disable monitoring
        if (c->monitoring_enabled) {
            swclock_enable_monitoring(c, false);
        }
        
        // Close JSON-LD logger
        if (c->jsonld_logger) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            uint64_t timestamp_ns = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
            swclock_jsonld_log_system(c->jsonld_logger, timestamp_ns, "swclock_stop", "{}");
            swclock_jsonld_close(c->jsonld_logger);
            c->jsonld_logger = NULL;
        }
    }
    
    pthread_rwlock_destroy(&c->lock);
    
    free(c);
}

int swclock_gettime(SwClock* c, clockid_t clk_id, struct timespec *tp) {
    
    if (!c || !tp) 
    { 
        errno = EINVAL; 
        return -1;
    }

    if (clk_id == CLOCK_MONOTONIC_RAW) {
        return clock_gettime(CLOCK_MONOTONIC_RAW, tp);
    }

    // Acquire read lock (non-exclusive) - multiple gettime() calls can proceed concurrently
    // Poll thread will not update while any reader holds the lock
    pthread_rwlock_rdlock(&c->lock);

    // Read all values atomically
    int64_t base_ns;
    switch (clk_id) {
        case CLOCK_REALTIME:
            base_ns = c->base_rt_ns;
            break;
        case CLOCK_MONOTONIC:
            base_ns = c->base_mono_ns;
            break;
        default:
            pthread_rwlock_unlock(&c->lock);
            errno = EINVAL;
            return -1;
    }
    
    struct timespec ref_time = c->ref_mono_raw;
    double factor = c->cached_total_factor;
    
    pthread_rwlock_unlock(&c->lock);
    
    // Extrapolate current time from last poll state (outside lock)
    struct timespec now_raw;
    clock_gettime(CLOCK_MONOTONIC_RAW, &now_raw);
    
    int64_t elapsed_raw_ns = ts_to_ns(&now_raw) - ts_to_ns(&ref_time);
    if (elapsed_raw_ns < 0) elapsed_raw_ns = 0;
    
    int64_t adj_elapsed_ns = (int64_t)((double)elapsed_raw_ns * factor);
    int64_t current_ns = base_ns + adj_elapsed_ns;
    
    *tp = ns_to_ts(current_ns);
    return 0;
}

int swclock_settime(SwClock* c, clockid_t clk_id, const struct timespec *tp) {
    if (!c || !tp) { errno = EINVAL; return -1; }
    if (clk_id != CLOCK_REALTIME) { errno = EINVAL; return -1; }

    pthread_rwlock_wrlock(&c->lock);
    swclock_rebase_now_and_update(c);
    c->base_rt_ns = (tp->tv_sec < 0) ? 0 : ts_to_ns(tp);
    // When the user sets time explicitly, clear leftover corrections
    c->remaining_phase_ns = 0;
    c->pi_int_error_s = 0.0;
    c->pi_freq_ppm = 0.0;
    pthread_rwlock_unlock(&c->lock);
    return 0;
}

int swclock_adjtime(SwClock* c, struct timex *tptr) {
    if (!c || !tptr) { errno = EINVAL; return -1; }

    // Log adjtime call event
    swclock_event_adjtime_payload_t adj_payload_entry = {
        .modes = tptr->modes,
        .offset_ns = (tptr->modes & ADJ_NANO) ? tptr->offset : tptr->offset * 1000LL,
        .freq_scaled_ppm = (tptr->modes & ADJ_FREQUENCY) ? tptr->freq : 0,
        .return_code = -1
    };
    swclock_log_event(c, SWCLOCK_EVENT_ADJTIME_CALL, &adj_payload_entry, sizeof(adj_payload_entry));

    pthread_rwlock_wrlock(&c->lock);
    swclock_rebase_now_and_update(c);

    unsigned int modes = tptr->modes;

    /* Base frequency bias (Darwin uses same scaled units: ppm * 2^-16) */
    if (modes & ADJ_FREQUENCY) {
        c->freq_scaled_ppm = tptr->freq;
        
        // JSON-LD logging
        if (c->jsonld_logger) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            uint64_t timestamp_ns = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
            swclock_jsonld_log_adjustment(c->jsonld_logger, timestamp_ns,
                "frequency_adjust", scaledppm_to_ppm(tptr->freq), 0, 0);
        }
    }

    /* ADJ_OFFSET: SLEW the phase via PI (no immediate step) */
    if (modes & ADJ_OFFSET) {
        long long delta_ns;
        if (modes & ADJ_NANO) {
            delta_ns = (long long)tptr->offset;          // already ns
        } else {
            delta_ns = (long long)tptr->offset * 1000LL; // usec -> ns
        }
        long long before_phase = c->remaining_phase_ns;
        c->remaining_phase_ns += delta_ns;               // PI will work this down
        c->pi_int_error_s = 0.0;
        c->pi_freq_ppm    = 0.0;
        
        // JSON-LD logging
        if (c->jsonld_logger) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            uint64_t timestamp_ns = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
            swclock_jsonld_log_adjustment(c->jsonld_logger, timestamp_ns,
                "slew", delta_ns / 1000000000.0, before_phase, c->remaining_phase_ns);
        }
    }

    /* ADJ_SETOFFSET: RELATIVE STEP (immediate)
       macOS headers vary; prefer timex.time if nonzero, else fall back to offset. */
    if (modes & ADJ_SETOFFSET) {
        long long delta_ns = 0;

        /* Try to use timex.time if available & nonzero */
        /* Some Darwin SDKs do declare .time; others don't. If present but zero,
           we still want to accept 'offset' so tests pass in both cases. */
        #define TIMEX_TIME_NONZERO ( (tptr->time.tv_sec != 0) || (tptr->time.tv_usec != 0) )
        #ifdef __APPLE__
        if (TIMEX_TIME_NONZERO) {
            long long tv_nsec;
            if (modes & ADJ_NANO) {
                /* With ADJ_NANO, Linux uses tv_usec to carry nanoseconds */
                tv_nsec = (long long)tptr->time.tv_usec;
            } else {
                tv_nsec = (long long)tptr->time.tv_usec * 1000LL; // usec -> ns
            }
            delta_ns = (long long)tptr->time.tv_sec * 1000000000LL + tv_nsec;
        } else
        #endif
        {
        
        // JSON-LD logging
        if (c->jsonld_logger) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            uint64_t timestamp_ns = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
            swclock_jsonld_log_adjustment(c->jsonld_logger, timestamp_ns,
                "phase_step", delta_ns / 1000000000.0, -delta_ns, 0);
        }
            /* Fallback: treat 'offset' as relative step */
            if (modes & ADJ_NANO) {
                delta_ns = (long long)tptr->offset;          // ns
            } else {
                delta_ns = (long long)tptr->offset * 1000LL; // usec -> ns
            }
        }

        /* Immediate step of REALTIME base */
        c->base_rt_ns += delta_ns;
        
        /* CRITICAL: Reset PI servo state after immediate step
         * An immediate step changes the time base discontinuously, making any
         * prior phase correction target obsolete. The servo must not continue
         * trying to correct the old remaining_phase_ns value.
         * However, we keep freq_scaled_ppm intact as it represents long-term drift.
         */
        c->remaining_phase_ns = 0;
        c->pi_int_error_s = 0.0;
    }

    /* Optional pass-through of status flags */
    if (modes & ADJ_STATUS) {
        c->status = tptr->status;
    }

    /* Optional: TAI-UTC offset if you use it */
    if (modes & ADJ_TAI) {
        c->tai = tptr->constant;
    }

    /* Readback (adjtimex-like) */
    tptr->status    = c->status;
    tptr->freq      = c->freq_scaled_ppm;
    tptr->maxerror  = c->maxerror;
    tptr->esterror  = c->esterror;
    tptr->constant  = c->constant;
    tptr->precision = 1;
    tptr->tick      = c->tick;
    tptr->tai       = c->tai;

    pthread_rwlock_unlock(&c->lock);
    
    // Log adjtime return event
    swclock_event_adjtime_payload_t adj_payload_return = {
        .modes = modes,
        .offset_ns = 0,
        .freq_scaled_ppm = c->freq_scaled_ppm,
        .return_code = TIME_OK
    };
    swclock_log_event(c, SWCLOCK_EVENT_ADJTIME_RETURN, &adj_payload_return, sizeof(adj_payload_return));
    
    return TIME_OK;
}

void swclock_reset(SwClock* c) {

    /* IMPORTANT: Not thread-safe use pthread_rwlock_wrlock(&c->lock); to call this function */
    if (!c) return;

    clock_gettime(CLOCK_MONOTONIC_RAW, &c->ref_mono_raw);

    struct timespec sys_rt = {0}, sys_mono_raw = {0};
    clock_gettime(CLOCK_REALTIME, &sys_rt);
    clock_gettime(CLOCK_MONOTONIC_RAW, &sys_mono_raw);

    c->base_rt_ns   = ts_to_ns(&sys_rt);
    c->base_mono_ns = ts_to_ns(&sys_mono_raw);

    c->freq_scaled_ppm    = 0;
    c->pi_freq_ppm        = 0.0;
    c->pi_int_error_s     = 0.0;
    c->remaining_phase_ns = 0;

    c->status   = 0; 
    c->maxerror = 0; 
    c->esterror = 0; 
    c->constant = 0; 
    c->tick     = 0; 
    c->tai      = 0;

    c->stop_flag           = false;
    c->poll_thread_running = true;

    // c->pi_servo_enabled: Not reset, we respect the previous state
}


// ================= Background thread =================

static void* swclock_poll_thread_main(void* arg) {
    SwClock* c = (SwClock*)arg;
    struct timespec ts = { .tv_sec = 0, .tv_nsec = SWCLOCK_POLL_NS };

    while (1) {
        // Sleep first to avoid a busy loop
        nanosleep(&ts, NULL);

        pthread_rwlock_wrlock(&c->lock);
        bool stop = c->stop_flag;
        pthread_rwlock_unlock(&c->lock);
        if (stop) break;

        swclock_poll(c);

        // Conditional servo state logging (enabled via SWCLOCK_SERVO_LOG env var)
        // NOTE: This logging is for debugging/audit purposes and has minimal overhead
        // when disabled (flag checked once at swclock_create time)
        if (c->servo_log_enabled) {
            pthread_rwlock_wrlock(&c->lock);
            if (c->log_fp && c->is_logging) {
                swclock_log(c);  // ENABLED: Priority 1 Recommendation 5
            }
            pthread_rwlock_unlock(&c->lock);
        }
        
        // JSON-LD ServoStateUpdate logging (independent of CSV logging)
        // Read servo state inside lock, then log outside to avoid blocking
        if (c->jsonld_logger && c->servo_log_enabled) {
            double freq_ppm_snapshot;
            int64_t phase_error_ns_snapshot;
            int64_t time_error_ns_snapshot;
            double pi_freq_ppm_snapshot;
            double pi_int_error_s_snapshot;
            bool servo_enabled_snapshot;
            uint64_t timestamp_ns;
            
            pthread_rwlock_wrlock(&c->lock);
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            timestamp_ns = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
            
            // Calculate phase and time errors  
            phase_error_ns_snapshot = c->remaining_phase_ns;
            
            // Calculate SwClock time directly (don't call swclock_gettime while holding lock)
            struct timespec now_raw;
            clock_gettime(CLOCK_MONOTONIC_RAW, &now_raw);
            int64_t elapsed_raw_ns = ts_to_ns(&now_raw) - ts_to_ns(&c->ref_mono_raw);
            if (elapsed_raw_ns < 0) elapsed_raw_ns = 0;
            int64_t adj_elapsed_ns = (int64_t)((double)elapsed_raw_ns * c->cached_total_factor);
            int64_t sw_time_ns = c->base_rt_ns + adj_elapsed_ns;
            int64_t sys_time_ns = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
            time_error_ns_snapshot = sys_time_ns - sw_time_ns;
            
            // Snapshot other servo state
            freq_ppm_snapshot = scaledppm_to_ppm(c->freq_scaled_ppm);
            pi_freq_ppm_snapshot = c->pi_freq_ppm;
            pi_int_error_s_snapshot = c->pi_int_error_s;
            servo_enabled_snapshot = c->pi_servo_enabled;
            pthread_rwlock_unlock(&c->lock);
            
            // Log outside the critical section to avoid blocking other threads
            swclock_jsonld_log_servo(c->jsonld_logger, timestamp_ns,
                freq_ppm_snapshot,
                phase_error_ns_snapshot, time_error_ns_snapshot,
                pi_freq_ppm_snapshot, pi_int_error_s_snapshot,
                servo_enabled_snapshot);
        }
        
        // Real-time monitoring: Add TE sample to circular buffer (Rec 7)
        if (c->monitoring_enabled && c->monitor) {
            // Compute Time Error: Reference_Time - SwClock_Disciplined_Time
            // Both must be in the same time domain (REALTIME)
            
            // Reference: System's CLOCK_REALTIME (undisciplined)
            struct timespec sys_realtime;
            clock_gettime(CLOCK_REALTIME, &sys_realtime);
            int64_t ref_time_ns = ts_to_ns(&sys_realtime);
            
            // SwClock's disciplined REALTIME
            struct timespec sw_realtime;
            swclock_gettime(c, CLOCK_REALTIME, &sw_realtime);
            int64_t swclock_time_ns = ts_to_ns(&sw_realtime);
            
            // TE = Reference - SwClock (positive means SwClock is behind)
            int64_t te_ns = ref_time_ns - swclock_time_ns;
            
            // Use MONOTONIC_RAW for timestamp (monotonic, steady reference)
            struct timespec now_mono;
            clock_gettime(CLOCK_MONOTONIC_RAW, &now_mono);
            uint64_t timestamp_ns = (uint64_t)ts_to_ns(&now_mono);
            
            swclock_monitor_add_sample(c->monitor, timestamp_ns, te_ns);
        }
    }
    return NULL;
}

void swclock_enable_PIServo(SwClock* c)
{
    if (!c) return;

    if (true != c->pi_servo_enabled) {
        pthread_rwlock_wrlock(&c->lock);
        c->pi_servo_enabled = true;
        c->pi_int_error_s   = 0.0;
        c->pi_freq_ppm      = 0.0;
        pthread_rwlock_unlock(&c->lock);
        
        // Log PI enable event
        swclock_log_event(c, SWCLOCK_EVENT_PI_ENABLE, NULL, 0);
    }
}


void swclock_disable_PIServo(SwClock* c)
{
    if (!c) return;

    if (true == c->pi_servo_enabled) {
        pthread_rwlock_wrlock(&c->lock);
        c->pi_servo_enabled = false;
        c->pi_int_error_s   = 0.0;
        c->pi_freq_ppm      = 0.0;
        pthread_rwlock_unlock(&c->lock);
    }
}


bool swclock_is_PIServo_enabled(SwClock* c)
{
    if (!c) return false;
    bool is_enabled = true;

    pthread_rwlock_wrlock(&c->lock);
    is_enabled = c->pi_servo_enabled;
    pthread_rwlock_unlock(&c->lock);
    
    return is_enabled;
}


// ================= Logging support =================

void swclock_start_log(SwClock* c, const char* filename) {
    if (!c || !filename) return;

    pthread_rwlock_wrlock(&c->lock);

    c->log_fp = fopen(filename, "w");
    if (!c->log_fp) {
        perror("swclock_start_log: fopen");
        pthread_rwlock_unlock(&c->lock);
        return;
    }

    // Get current local date and time
    time_t now = time(NULL);
    struct tm* tinfo = localtime(&now);
    char datetime_buf[64];
    strftime(datetime_buf, sizeof(datetime_buf), "%Y-%m-%d %H:%M:%S", tinfo);

    // Write header with version and timestamp
    fprintf(c->log_fp,
        "# SwClock Log (%s)\n"
        "# Version: %s\n"
        "# Started at: %s\n"
        "# Columns:\n"
        "timestamp_ns,"
        "base_rt_ns,"
        "base_mono_ns,"
        "freq_scaled_ppm,"
        "pi_freq_ppm,"
        "pi_int_error_s,"
        "remaining_phase_ns,"
        "pi_servo_enabled,"
        "maxerror,"
        "esterror,"
        "constant,"
        "tick,"
        "tai\n",
        filename,
        SWCLOCK_VERSION,
        datetime_buf
    );

    fflush(c->log_fp);
    c->is_logging = true;

    pthread_rwlock_unlock(&c->lock);
    
    // Automatically start event logging if SWCLOCK_EVENT_LOG is set
    if (getenv("SWCLOCK_EVENT_LOG") != NULL) {
        // Generate event log filename based on CSV filename
        char event_log_path[512];
        snprintf(event_log_path, sizeof(event_log_path), "logs/events_%s.bin", datetime_buf);
        
        // Replace colons and spaces in the filename for filesystem compatibility
        for (char* p = event_log_path; *p; p++) {
            if (*p == ':' || *p == ' ') *p = '-';
        }
        
        if (swclock_start_event_log(c, event_log_path) == 0) {
            printf("Event logging started: %s\n", event_log_path);
        } else {
            fprintf(stderr, "Warning: Failed to start event logging\n");
        }
    }
}

void swclock_log(SwClock* c) {
    if (!c || !c->is_logging || !c->log_fp) return;

    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC_RAW, &now);
    long long now_ns = ts_to_ns(&now);

    fprintf(c->log_fp,
        "%lld,"        // timestamp
        "%" PRId64 "," // base_rt_ns
        "%" PRId64 "," // base_mono_ns
        "%ld,"         // freq_scaled_ppm
        "%.9f,"        // pi_freq_ppm
        "%.9f,"        // pi_int_error_s
        "%lld,"        // remaining_phase_ns
        "%d,"          // pi_servo_enabled
        "%ld,"         // maxerror
        "%ld,"         // esterror
        "%ld,"         // constant
        "%ld,"         // tick
        "%d\n",        // tai
        now_ns,
        c->base_rt_ns,
        c->base_mono_ns,
        c->freq_scaled_ppm,
        c->pi_freq_ppm,
        c->pi_int_error_s,
        c->remaining_phase_ns,
        c->pi_servo_enabled ? 1 : 0,
        c->maxerror,
        c->esterror,
        c->constant,
        c->tick,
        c->tai);

    fflush(c->log_fp);
    
    // JSON-LD servo state logging
    if (c->jsonld_logger) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        uint64_t timestamp_ns = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
        
        // Calculate phase and time errors for the log
        int64_t phase_error_ns = c->remaining_phase_ns;
        // Time error: compare system REALTIME to SwClock REALTIME
        struct timespec sys_realtime, sw_realtime;
        clock_gettime(CLOCK_REALTIME, &sys_realtime);
        swclock_gettime(c, CLOCK_REALTIME, &sw_realtime);
        int64_t time_error_ns = ts_to_ns(&sys_realtime) - ts_to_ns(&sw_realtime);
        
        swclock_jsonld_log_servo(c->jsonld_logger, timestamp_ns,
            scaledppm_to_ppm(c->freq_scaled_ppm),
            phase_error_ns, time_error_ns,
            c->pi_freq_ppm, c->pi_int_error_s,
            c->pi_servo_enabled);
    }
}

void swclock_close_log(SwClock* c) {
    if (!c) return;
    pthread_rwlock_wrlock(&c->lock);
    if (c->log_fp) {
        fclose(c->log_fp);
        c->log_fp = NULL;
    }
    c->is_logging = false;
    pthread_rwlock_unlock(&c->lock);
}

// ================= Event Logging (Priority 1 Recommendation 2) =================

// Forward declaration
static void* swclock_event_logger_thread_main(void* arg);

static uint64_t swclock_get_timestamp_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

int swclock_start_event_log(SwClock* c, const char* filename) {
    if (!c || !filename) return -1;
    
    pthread_rwlock_wrlock(&c->lock);
    
    // Already logging
    if (c->event_logging_enabled) {
        pthread_rwlock_unlock(&c->lock);
        return -1;
    }
    
    // Open binary log file
    c->event_log_fp = fopen(filename, "wb");
    if (!c->event_log_fp) {
        pthread_rwlock_unlock(&c->lock);
        return -1;
    }
    
    // Write file header
    swclock_event_log_header_t header = {
        .magic = SWCLOCK_EVENT_LOG_MAGIC,
        .version_major = 1,
        .version_minor = 0,
        .start_time_ns = swclock_get_timestamp_ns()
    };
    strncpy(header.swclock_version, SWCLOCK_VERSION, sizeof(header.swclock_version) - 1);
    
    if (fwrite(&header, sizeof(header), 1, c->event_log_fp) != 1) {
        fclose(c->event_log_fp);
        c->event_log_fp = NULL;
        pthread_rwlock_unlock(&c->lock);
        return -1;
    }
    fflush(c->event_log_fp);
    
    // Initialize ring buffer
    swclock_ringbuf_init(&c->event_ringbuf);
    c->event_sequence = 0;
    c->event_logging_enabled = true;
    
    // Start background logger thread
    c->event_logger_running = true;
    if (pthread_create(&c->event_logger_thread, NULL,
                      swclock_event_logger_thread_main, c) != 0) {
        c->event_logging_enabled = false;
        c->event_logger_running = false;
        fclose(c->event_log_fp);
        c->event_log_fp = NULL;
        pthread_rwlock_unlock(&c->lock);
        return -1;
    }
    
    pthread_rwlock_unlock(&c->lock);
    
    // Log start event
    swclock_log_event(c, SWCLOCK_EVENT_LOG_START, NULL, 0);
    
    return 0;
}

void swclock_stop_event_log(SwClock* c) {
    if (!c) return;
    
    pthread_rwlock_wrlock(&c->lock);
    
    if (!c->event_logging_enabled) {
        pthread_rwlock_unlock(&c->lock);
        return;
    }
    
    // Log stop event
    pthread_rwlock_unlock(&c->lock);
    swclock_log_event(c, SWCLOCK_EVENT_LOG_STOP, NULL, 0);
    pthread_rwlock_wrlock(&c->lock);
    
    // Stop logger thread
    c->event_logger_running = false;
    pthread_rwlock_unlock(&c->lock);
    
    pthread_join(c->event_logger_thread, NULL);
    
    pthread_rwlock_wrlock(&c->lock);
    
    // Close file
    if (c->event_log_fp) {
        fclose(c->event_log_fp);
        c->event_log_fp = NULL;
    }
    
    c->event_logging_enabled = false;
    
    pthread_rwlock_unlock(&c->lock);
}

void swclock_log_event(SwClock* c, swclock_event_type_t event_type,
                      const void* payload, size_t payload_size) {
    if (!c || !c->event_logging_enabled) return;
    
    // Build event header
    swclock_event_header_t header = {
        .sequence_num = __atomic_fetch_add(&c->event_sequence, 1, __ATOMIC_SEQ_CST),
        .timestamp_ns = swclock_get_timestamp_ns(),
        .event_type = event_type,
        .payload_size = (uint16_t)payload_size,
        .reserved = 0
    };
    
    // Combine header + payload into single buffer
    uint8_t event_buffer[SWCLOCK_EVENT_MAX_SIZE];
    memcpy(event_buffer, &header, sizeof(header));
    if (payload && payload_size > 0) {
        memcpy(event_buffer + sizeof(header), payload, payload_size);
    }
    
    // Push to ring buffer (non-blocking)
    swclock_ringbuf_push(&c->event_ringbuf, event_buffer,
                        sizeof(header) + payload_size);
}

static void* swclock_event_logger_thread_main(void* arg) {
    SwClock* c = (SwClock*)arg;
    uint8_t event_buffer[SWCLOCK_EVENT_MAX_SIZE];
    
    while (c->event_logger_running || !swclock_ringbuf_is_empty(&c->event_ringbuf)) {
        size_t event_size;
        
        // Pop event from ring buffer
        if (swclock_ringbuf_pop(&c->event_ringbuf, event_buffer,
                               SWCLOCK_EVENT_MAX_SIZE, &event_size)) {
            // Write to file
            pthread_rwlock_wrlock(&c->lock);
            if (c->event_log_fp) {
                fwrite(event_buffer, 1, event_size, c->event_log_fp);
                fflush(c->event_log_fp);
            }
            pthread_rwlock_unlock(&c->lock);
        } else {
            // No events available, sleep briefly
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 }; // 1ms
            nanosleep(&ts, NULL);
        }
        
        // Check for overruns
        if (swclock_ringbuf_clear_overrun(&c->event_ringbuf)) {
            fprintf(stderr, "swclock: Event ring buffer overrun detected\n");
        }
    }
    
    return NULL;
}

// ================= Real-Time Monitoring (Rec 7) =================

int swclock_enable_monitoring(SwClock* c, bool enable) {
    if (!c) {
        errno = EINVAL;
        return -1;
    }
    
    pthread_rwlock_wrlock(&c->lock);
    
    if (enable && !c->monitoring_enabled) {
        // Allocate and initialize monitor
        c->monitor = malloc(sizeof(swclock_monitor_t));
        if (!c->monitor) {
            pthread_rwlock_unlock(&c->lock);
            return -1;
        }
        
        // Initialize monitor with 10 Hz sample rate (SWCLOCK_POLL_HZ)
        if (swclock_monitor_init(c->monitor, 100.0) != 0) {
            free(c->monitor);
            c->monitor = NULL;
            pthread_rwlock_unlock(&c->lock);
            return -1;
        }
        
        // Start background computation thread
        if (swclock_monitor_start_compute_thread(c->monitor) != 0) {
            swclock_monitor_destroy(c->monitor);
            free(c->monitor);
            c->monitor = NULL;
            pthread_rwlock_unlock(&c->lock);
            return -1;
        }
        
        c->monitoring_enabled = true;
        
    } else if (!enable && c->monitoring_enabled) {
        // Disable monitoring
        if (c->monitor) {
            swclock_monitor_destroy(c->monitor);
            free(c->monitor);
            c->monitor = NULL;
        }
        
        c->monitoring_enabled = false;
    }
    
    pthread_rwlock_unlock(&c->lock);
    return 0;
}

int swclock_get_metrics(SwClock* c, swclock_metrics_snapshot_t* snapshot) {
    if (!c || !snapshot) {
        errno = EINVAL;
        return -1;
    }
    
    if (!c->monitoring_enabled || !c->monitor) {
        errno = ENOTSUP;
        return -1;
    }
    
    return swclock_monitor_get_metrics(c->monitor, snapshot);
}

void swclock_set_thresholds(SwClock* c, const swclock_threshold_config_t* config) {
    if (!c || !config) return;
    
    pthread_rwlock_wrlock(&c->lock);
    
    if (c->monitoring_enabled && c->monitor) {
        swclock_monitor_set_thresholds(c->monitor, config);
    }
    
    pthread_rwlock_unlock(&c->lock);
}
