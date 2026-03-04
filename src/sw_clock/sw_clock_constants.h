//
// sw_clock_constants.h
// Constants used by the SwClock implementation
//
#ifndef SW_CLOCK_CONSTANTS_H
#define SW_CLOCK_CONSTANTS_H

#ifdef __cplusplus
extern "C" {
#endif

// Constants used by the ntp_adjtime() frequency field
#define NTP_SCALE_SHIFT   16                // Frequency units are (ppm << 16)
#define NTP_SCALE_FACTOR  (1L << NTP_SCALE_SHIFT)  // 65536

// Units
#define PPM_TO_PPB   1000LL       // parts-per-million to parts-per-billion  
#define NS_PER_SEC   1000000000LL
#define US_PER_SEC   1000000LL
#define MS_PER_SEC   1000LL

#define NS_PER_US    1000LL
#define NS_PER_MS    1000000LL
#define SEC_PER_NS   (1.0 / (double)NS_PER_SEC)

// ================= Configuration =================

// Polling period for the background thread (nanoseconds)
#define SWCLOCK_POLL_NS          10*1000*1000L  // 10 ms (100 Hz)

// PI controller gains (units explained below)
#define SWCLOCK_PI_KP_PPM_PER_S  200.0   // ppm/s For 1 s of phase error, command ~200 ppm
#define SWCLOCK_PI_KI_PPM_PER_S2   8.0   // ppm/s² Integral gain (ppm per s^2)

// Limit the PI frequency correction (in ppm)
#define SWCLOCK_PI_MAX_PPM       200.0   // ppm conservative default

// When the remaining phase error magnitude drops below this, zero the PI
#define SWCLOCK_PHASE_EPS_NS     20000LL   // 20 µs


#ifdef __cplusplus
} // extern "C"
#endif

#endif // SW_CLOCK_CONSTANTS_H
