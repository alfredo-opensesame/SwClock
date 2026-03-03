//
// sw_clock.c
// Software clock for macOS driven by CLOCK_MONOTONIC_RAW.
// Implements Linux-like gettime/settime/adjtime semantics suitable for PTPd.
//

// Prevent system timex.h inclusion to avoid conflicts with our definitions
#ifndef __SYS_TIMEX_H__
#define __SYS_TIMEX_H__
#endif

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "sw_clock.h"

// Force IntelliSense to see our definitions (redundant but fixes IDE warnings)
#ifndef ADJ_FREQUENCY
#define ADJ_FREQUENCY   0x0002
#define ADJ_OFFSET      0x0001  
#define ADJ_NANO        0x2000
#define ADJ_SETOFFSET   0x0100
#define ADJ_STATUS      0x0010
#endif

// -------- helpers -------------------------------------------

void print_timespec_as_datetime(const struct timespec *ts)
{
    if (!ts) {
        fprintf(stderr, "print_timespec_as_datetime: null pointer\n");
        return;
    }

    char buf[64];
    struct tm tm_utc;

    // Convert seconds to broken-down UTC time
    gmtime_r(&ts->tv_sec, &tm_utc);

    // Format as "YYYY-MM-DD HH:MM:SS"
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_utc);

    // Print with nanoseconds
    printf("%s.%09ld UTC\n", buf, ts->tv_nsec);
}

void print_timespec_as_localtime(const struct timespec *ts)
{
    if (!ts) {
        fprintf(stderr, "print_timespec_as_localtime: null pointer\n");
        return;
    }

    // Defensive normalization: ensure tv_nsec within [0, 1e9)
    time_t sec = ts->tv_sec;
    long nsec = ts->tv_nsec;
    if (nsec >= NS_PER_SEC) {
        sec += nsec / NS_PER_SEC;
        nsec %= NS_PER_SEC;
    } else if (nsec < 0) {
        long borrow = (-nsec + NS_PER_SEC - 1) / NS_PER_SEC;
        sec -= borrow;
        nsec += borrow * NS_PER_SEC;
    }

    // Convert to local time
    struct tm tm_local;
#if defined(_POSIX_THREAD_SAFE_FUNCTIONS) && !defined(_WIN32)
    localtime_r(&sec, &tm_local);
#else
    struct tm *tmp = localtime(&sec);
    if (!tmp) return;
    tm_local = *tmp;
#endif

    // Format into "YYYY-MM-DD HH:MM:SS.NNNNNNNNN TZ"
    char buf[64];
    if (strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_local) == 0)
        return;

    // Append fractional seconds and timezone abbreviation
    char tz_abbr[32] = "";
#ifdef __APPLE__
    if (tm_local.tm_zone) snprintf(tz_abbr, sizeof(tz_abbr), "%s", tm_local.tm_zone);
#else
    strftime(tz_abbr, sizeof(tz_abbr), "%Z", &tm_local);
#endif
    printf("%s.%09ld %s\n", buf, nsec, tz_abbr);
}

/* Future of leap seconds
 * At the 2022 World Radiocommunication Conference, it was agreed that:
 * Leap seconds will be suspended starting 2035 for at least a century.
 * That means the TAI–UTC offset will remain fixed (currently +37 s) for 
 * decades unless a new scheme is adopted later.
 */
#define TAI_OFFSET_2025 37  // seconds ahead of UTC as of Oct 2025

void print_timespec_as_TAI(const struct timespec *ts)
{
    if (!ts) {
        fprintf(stderr, "print_timespec_as_TAI: null pointer\n");
        return;
    }

    // Normalize tv_nsec and apply TAI offset
    time_t sec = ts->tv_sec + TAI_OFFSET_2025;
    long nsec = ts->tv_nsec;

    if (nsec >= NS_PER_SEC) {
        sec += nsec / NS_PER_SEC;
        nsec %= NS_PER_SEC;
    } else if (nsec < 0) {
        long borrow = (-nsec + NS_PER_SEC - 1) / NS_PER_SEC;
        sec -= borrow;
        nsec += borrow * NS_PER_SEC;
    }

    // Convert to broken-down UTC time (TAI is UTC + offset, but shares epoch)
    struct tm tm_tai;
#if defined(_POSIX_THREAD_SAFE_FUNCTIONS) && !defined(_WIN32)
    gmtime_r(&sec, &tm_tai);
#else
    struct tm *tmp = gmtime(&sec);
    if (!tmp) return;
    tm_tai = *tmp;
#endif

    // Format date/time (ISO 8601-like)
    char buf[64];
    if (strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_tai) == 0)
        return;

    printf("%s.%09ld TAI (+%ds)\n", buf, nsec, TAI_OFFSET_2025);
}
