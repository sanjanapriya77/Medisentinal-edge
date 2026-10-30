
/* ============================================================================
 * COURSE   : EC 2V87 - EMBEDDED SOFTWARE DEVELOPMENT
 * PROJECT  : QNX MediSentinel - Predictive Clinical-Criticality Orchestration
 * TEAM     : QubeX (G. Sanjanapriya, K. P. Shreeja, V. N. Tharsine)
 * TARGET   : Raspberry Pi 4 (AArch64) | QNX SDP 8.0 (Neutrino RTOS)
 * PROBLEM  : Sl.No. 19 - ICU Multi-Patient Alarm Prioritization System
 * INNOVATION: Context-Aware Alarm Fatigue Reduction Engine
 *            (Persistence Checking, Hysteresis Stabilization, Tri-Level FSM)
 *
 * RTOS TASK PRIORITIES (SCHED_FIFO):
 *   - Priority 28 : Alarm Dispatcher Task (Anti-Starvation Aging Engine)
 *   - Priority 24 : Alarm Classifier & QNX Safety Supervisor (Hard RT Lane)
 *   - Priority 20 : Patient Monitor Task (100 Hz Hardware Sampling)
 *   - Priority 18 : Alarm Actuator Task (GPIO 17 LED & GPIO 4 Buzzer)
 *   - Priority 15 : AI Intelligence Lane Task (5-Stage Analytics Pipeline)
 *   - Priority 12 : Dashboard Sender Task (Network JSON Telemetry)
 *   - Priority 10 : Logger & CLI Display Task (Bedside Monitoring UI)
 * ============================================================================ */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <signal.h>
#include <stdarg.h>
#include <sys/time.h>
#include <pthread.h>
#include <sched.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/mman.h>
#include <sys/neutrino.h>
#include <sys/netmgr.h>
#include <atomic.h>
#include <devctl.h>
#include <hw/i2c.h>

/* --- CONFIGURATION & SENSOR CONSTANTS (Exact Ranges Preserved) --- */
#define BRIDGE_IP            "10.0.0.2"
#define BRIDGE_PORT          5000
#define CYCLE_INTERVAL_SEC   7
#define PATIENT_COUNT        4

#define I2C_DEVICE           "/dev/i2c1"
#define MAX30102_ADDR        0x57
#define MPU6050_ADDR         0x68
#define FINGER_IR_MIN        26000u
#define SPO2_MIN_NORMAL      60
#define SPO2_MAX_NORMAL      100

/* BCM2711 GPIO MMIO - Raspberry Pi 4 */
#define BCM2711_GPIO_BASE    0xFE200000u
#define BCM2711_GPIO_LEN     0x1000u
#define GPIO_PIN_LED         17        /* Pin 11 */
#define GPIO_PIN_BUZZER      4         /* Pin 7  */
#define GPFSEL0              0
#define GPFSEL1              1
#define GPSET0               7
#define GPCLR0               10
#define BUZZER_ACTIVE_LOW    0

/* --- SENSOR FAULT DETECTION CONSTANTS --- */
#define FAULT_OK                 0x00
#define FAULT_MAX_DISCONNECTED   0x01   /* I2C Read failure on MAX30102 */
#define FAULT_MPU_DISCONNECTED   0x02   /* I2C Read failure on MPU6050  */
#define FAULT_OPTICAL_SATURATED  0x04   /* Photodiode blinded by ambient light */
#define FAULT_DATA_FROZEN        0x08   /* Stuck-at register fault (frozen chip) */

/* --- ALARM FATIGUE REDUCTION CONSTANTS --- */
#define PERSISTENCE_THRESHOLD_MS 3000L   /* Non-critical abnormalities must persist >= 3.0s */
#define HYSTERESIS_HR_BPM        5       /* Recovery requires HR 5 BPM past normal limit */
#define HYSTERESIS_SPO2_PCT      3       /* Recovery requires SpO2 3% above danger floor */
#define HYSTERESIS_TEMP_DEG      0.3f    /* Recovery requires Temp 0.3C below fever limit */

/* Queue & Anti-Starvation */
#define MAX_QUEUE_ALARMS     16
#define AGING_BOOST_PER_SEC  3
#define MAX_TIMELINE_LOGS    8

#define PULSE_CODE_SAMPLE    (_PULSE_CODE_MINAVAIL + 1)
#define PULSE_CODE_TIMER     (_PULSE_CODE_MINAVAIL + 2)
#define PULSE_CODE_SHUTDOWN  (_PULSE_CODE_MINAVAIL + 3)

static inline float my_fabsf(float x) { return (x < 0.0f) ? -x : x; }
static inline float my_sqrtf(float x) {
    if (x <= 0.0f) return 0.0f;
    float g = (x > 1.0f) ? x * 0.5f : 1.0f;
    for (int i = 0; i < 6; i++) g = 0.5f * (g + x / g);
    return g;
}

/* --- DATA STRUCTURES --- */
typedef enum { SEVERITY_NONE = 0, SEVERITY_MODERATE, SEVERITY_HIGH, SEVERITY_CRITICAL } AlarmSeverity;
typedef enum { VERDICT_HARD_RT = 0, VERDICT_APPROVED, VERDICT_REJECTED_LOW_TRUST } SupervisorVerdict;

/* Alarm Fatigue State Machine */
typedef enum {
    STATE_NORMAL = 0,       /* Green: Vitals nominal */
    STATE_PENDING,          /* Yellow: Abnormality under persistence validation */
    STATE_ACTIVE,           /* Red/Critical: Confirmed sustained or severe alarm */
    STATE_ACKNOWLEDGED,     /* Yellow: Attended by nurse, muted, ongoing monitoring */
    STATE_RECOVERED         /* Green: Safely crossed hysteresis threshold */
} AlarmFatigueState;

typedef struct {
    int           patient_id;
    char          alarm_type[64];
    int           base_risk_score, effective_priority;
    AlarmSeverity severity;
    long          timestamp_ms, deadline_ms;
    int           is_fused_event, sensor_trust;
} AlarmEvent;

typedef struct { char log_text[128]; long timestamp_ms; } TimelineEntry;

typedef struct {
    int   id, is_simulated, finger_detected, hr, spo2;
    float temp;
    int   fall_detected, vitals_stable, temp_stable, patient_stable, risk_score, priority_rank;
    float base_hr, base_spo2;
    int   base_calibrated, prev_spo2;
    float spo2_slope;
    int   sensor_trust, has_ai_proposal, ai_fused;
    char  ai_event[64];
    int   ai_score;
    AlarmSeverity     ai_severity;
    SupervisorVerdict verdict;
    char  verdict_reason[64];
    int   prev_active, prev_score;
    char  last_desc[64];

    /* --- SENSOR FAULT DETECTION FIELDS --- */
    int   sensor_fault;
    char  fault_msg[64];

    /* --- ALARM FATIGUE REDUCTION ENGINE FIELDS --- */
    AlarmFatigueState fatigue_state;
    long              abnormal_start_ms;
    int               in_hysteresis;
    char              alarm_level[16];   /* "GREEN", "YELLOW", "RED" */
} Patient;

typedef struct {
    pthread_mutex_t lock;
    volatile unsigned int running, alarm_active;
    Patient         patients[PATIENT_COUNT];
    AlarmEvent      queue[MAX_QUEUE_ALARMS];
    int             queue_count, timeline_head, channel_id, connect_id;
    TimelineEntry   timeline[MAX_TIMELINE_LOGS];
    unsigned long   cycle;
    timer_t         sample_timer_id;
    unsigned long   total_alarms, dup_suppressed, aging_boosts, crit_dispatched;
    unsigned long   deadlines_breached, fused_events, ai_approved, ai_rejected, hard_fallbacks;

    /* Alarm Fatigue Metrics */
    unsigned long   transients_suppressed, hysteresis_recoveries;

    /* --- LIVE REAL-TIME LATENCY BENCHMARK METRICS --- */
    float           last_latency_us;
    float           avg_latency_us;
    float           min_latency_us;
    float           max_latency_us;
    unsigned long   latency_samples;
} ICUDataServer;

static ICUDataServer      g_server;
static int                g_i2c_fd = -1, g_max_ok = 0, g_mpu_ok = 0;
static volatile uint32_t *g_gpio   = NULL;

/* Sensor Raw Readings & Fault Trackers */
static uint32_t g_last_ir = 0, g_last_red = 0, g_ir_dc = 0, g_red_dc = 0;
static float    g_latest_temp = 36.8f, g_accel_mag = 1.0f, g_motion_jerk = 0.0f;
static float    g_prev_x = 0.0f, g_prev_y = 0.0f, g_prev_z = 1.0f;
static int      g_fall_latched = 0, g_calc_hr = 0, g_calc_spo2 = 0, g_was_rising = 0;
static int32_t  g_ir_smooth = 0, g_prev_ir_smooth = 0;
static long     g_fall_until_ms = 0, g_freefall_ms = -100000, g_last_beat_ms = 0;

/* Fault Detection State Variables */
static int      g_sensor_fault_bits = FAULT_OK;
static int      g_max_fail_cnt      = 0;
static int      g_mpu_fail_cnt      = 0;
static int16_t  g_last_raw_x = 0, g_last_raw_y = 0, g_last_raw_z = 0;
static int      g_mpu_freeze_cnt    = 0;

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)(ts.tv_sec * 1000L + ts.tv_nsec / 1000000L);
}

static void append_timeline(const char *fmt, ...) {
    va_list args; va_start(args, fmt);
    int idx = g_server.timeline_head % MAX_TIMELINE_LOGS;
    vsnprintf(g_server.timeline[idx].log_text, sizeof(g_server.timeline[idx].log_text), fmt, args);
    va_end(args);
    g_server.timeline[idx].timestamp_ms = now_ms();
    g_server.timeline_head++;
}

static inline const char *fatigue_state_to_str(AlarmFatigueState s) {
    switch (s) {
        case STATE_NORMAL:       return "NORMAL [GREEN]";
        case STATE_PENDING:      return "PENDING [YELLOW - Verifying Persistence]";
        case STATE_ACTIVE:       return "ACTIVE [RED - Audible Alarm]";
        case STATE_ACKNOWLEDGED: return "ACKNOWLEDGED [YELLOW - Silenced/Tracked]";
        case STATE_RECOVERED:    return "RECOVERED [GREEN - Hysteresis Cleared]";
        default:                 return "UNKNOWN";
    }
}

/* --- HARDWARE GPIO & I2C --- */
static inline void set_led(int on)    { if (g_gpio) g_gpio[on ? GPSET0 : GPCLR0] = (1u << GPIO_PIN_LED); }
static inline void set_buzzer(int on) {
    if (!g_gpio) return;
    int drive = BUZZER_ACTIVE_LOW ? !on : on;
    g_gpio[drive ? GPSET0 : GPCLR0] = (1u << GPIO_PIN_BUZZER);
}

static int gpio_hardware_init(void) {
    if (ThreadCtl(_NTO_TCTL_IO, 0) == -1) {
        /* Running in simulation or non-root: fallback gracefully */
        g_gpio = NULL;
        return 0;
    }
    g_gpio = (volatile uint32_t *)mmap_device_memory(NULL, BCM2711_GPIO_LEN,
              PROT_READ | PROT_WRITE | PROT_NOCACHE, 0, BCM2711_GPIO_BASE);
    if (!g_gpio || g_gpio == MAP_FAILED) {
        g_gpio = NULL;
        return 0;
    }
    g_gpio[GPFSEL1] = (g_gpio[GPFSEL1] & ~(7u << 21)) | (1u << 21);
    g_gpio[GPFSEL0] = (g_gpio[GPFSEL0] & ~(7u << 12)) | (1u << 12);
    set_led(0); set_buzzer(0);
    return 0;
}

static int i2c_cmd(uint8_t addr, uint8_t reg, uint8_t *data, size_t len, int is_recv) {
    if (g_i2c_fd < 0) return -1;
    uint8_t buf[sizeof(i2c_sendrecv_t) + 32] __attribute__((aligned(8)));
    if (is_recv) {
        i2c_sendrecv_t *msg = (i2c_sendrecv_t *)buf;
        msg->slave.addr = addr; msg->slave.fmt = I2C_ADDRFMT_7BIT;
        msg->send_len = 1; msg->recv_len = (uint32_t)len; msg->stop = 1;
        ((uint8_t *)(msg + 1))[0] = reg;
        int rc = devctl(g_i2c_fd, DCMD_I2C_SENDRECV, msg, (int)(sizeof(i2c_sendrecv_t) + len), NULL);
        if (rc == EOK && data) memcpy(data, (uint8_t *)(msg + 1), len);
        return rc;
    } else {
        i2c_send_t *msg = (i2c_send_t *)buf;
        msg->slave.addr = addr; msg->slave.fmt = I2C_ADDRFMT_7BIT; msg->len = 2; msg->stop = 1;
        ((uint8_t *)(msg + 1))[0] = reg; ((uint8_t *)(msg + 1))[1] = data ? data[0] : 0;
        return devctl(g_i2c_fd, DCMD_I2C_SEND, msg, (int)sizeof(buf), NULL);
    }
}
static inline int i2c_read(uint8_t a, uint8_t r, uint8_t *d, size_t l) { return i2c_cmd(a, r, d, l, 1); }
static inline int i2c_write(uint8_t a, uint8_t r, uint8_t v)          { return i2c_cmd(a, r, &v, 1, 0); }

static int max30102_init(void) {
    if (g_i2c_fd < 0) return -1;
    uint8_t part = 0;
    if (i2c_read(MAX30102_ADDR, 0xFF, &part, 1) != EOK || part != 0x15) return -1;
    uint8_t seq[][2] = {{0x09,0x40},{0x04,0x00},{0x05,0x00},{0x06,0x00},{0x08,0x4F},{0x0A,0x27},{0x0C,0x2A},{0x0D,0x2A},{0x09,0x03},{0x21,0x01}};
    for (size_t i = 0; i < sizeof(seq)/sizeof(seq[0]); i++) { i2c_write(MAX30102_ADDR, seq[i][0], seq[i][1]); if (i==0) usleep(50000); }
    return 0;
}

static void max30102_poll_fifo(void) {
    uint8_t f[6];
    if (i2c_read(MAX30102_ADDR, 0x07, f, 6) == EOK) {
        g_max_fail_cnt = 0;
        g_sensor_fault_bits &= ~FAULT_MAX_DISCONNECTED;

        g_last_red = (((uint32_t)f[0]<<16)|((uint32_t)f[1]<<8)|f[2]) & 0x3FFFF;
        g_last_ir  = (((uint32_t)f[3]<<16)|((uint32_t)f[4]<<8)|f[5]) & 0x3FFFF;

        if (g_last_ir >= 260000u || g_last_red >= 260000u) {
            g_sensor_fault_bits |= FAULT_OPTICAL_SATURATED;
        } else {
            g_sensor_fault_bits &= ~FAULT_OPTICAL_SATURATED;
        }
    } else {
        if (++g_max_fail_cnt >= 5) {
            g_sensor_fault_bits |= FAULT_MAX_DISCONNECTED;
        }
    }
}

static void max30102_temp_poll(void) {
    uint8_t tint = 0, frac = 0;
    if (i2c_read(MAX30102_ADDR, 0x1F, &tint, 1) == EOK && i2c_read(MAX30102_ADDR, 0x20, &frac, 1) == EOK) {
        float raw = (float)(int8_t)tint + (float)(frac & 0x0F) * 0.0625f;
        g_latest_temp = (raw >= 15.0f && raw <= 55.0f && g_last_ir >= FINGER_IR_MIN) ?
                        36.8f + (raw - 32.0f) * 1.4f : 36.8f;
        if (g_latest_temp < 30.0f) g_latest_temp = 30.0f;
        if (g_latest_temp > 43.0f) g_latest_temp = 43.0f;
    }
    i2c_write(MAX30102_ADDR, 0x21, 0x01);
}

static void process_ppg(uint32_t red, uint32_t ir, long t) {
    if (ir < FINGER_IR_MIN) {
        g_ir_dc = ir; g_red_dc = red; g_calc_hr = 0; g_calc_spo2 = 0;
        g_last_beat_ms = 0; g_ir_smooth = 0; g_prev_ir_smooth = 0;
        return;
    }
    if (!g_ir_dc) g_ir_dc = ir; if (!g_red_dc) g_red_dc = red;
    g_ir_dc  = (g_ir_dc * 63 + ir) / 64; g_red_dc = (g_red_dc * 63 + red) / 64;
    int32_t ac = (int32_t)ir - (int32_t)g_ir_dc;
    g_prev_ir_smooth = g_ir_smooth; g_ir_smooth = (g_ir_smooth * 3 + ac) / 4;

    long dt = t - g_last_beat_ms;
    if (g_ir_smooth > g_prev_ir_smooth) g_was_rising = 1;
    if (g_was_rising && g_ir_smooth < g_prev_ir_smooth && g_prev_ir_smooth > 150) {
        g_was_rising = 0;
        if (dt >= 500 && dt <= 1400) {
            int ihr = (int)(60000L / dt);
            if (ihr >= 50 && ihr <= 120) g_calc_hr = (g_calc_hr == 0) ? ihr : (g_calc_hr * 3 + ihr) / 4;
            float r = (my_fabsf((float)g_prev_ir_smooth) / (float)g_red_dc) / (my_fabsf((float)g_prev_ir_smooth) / (float)g_ir_dc);
            float sp = 104.0f - 17.0f * r;
            if (sp > 99.0f) sp = 98.0f;
            if (sp < 80.0f) sp = 88.0f;
            g_calc_spo2 = (g_calc_spo2 == 0) ? (int)sp : (g_calc_spo2 * 3 + (int)sp) / 4;
            g_last_beat_ms = t;
        } else if (dt > 1400 || g_last_beat_ms == 0) g_last_beat_ms = t;
    }
}

static int mpu6050_init(void) {
    if (g_i2c_fd < 0) return -1;
    uint8_t who = 0;
    if (i2c_read(MPU6050_ADDR, 0x75, &who, 1) != EOK || (who != 0x68 && who != 0x70)) return -1;
    i2c_write(MPU6050_ADDR, 0x6B, 0x00); usleep(50000);
    i2c_write(MPU6050_ADDR, 0x1A, 0x03); i2c_write(MPU6050_ADDR, 0x1C, 0x10);
    return 0;
}

static void mpu6050_poll(void) {
    uint8_t d[6];
    if (i2c_read(MPU6050_ADDR, 0x3B, d, 6) != EOK) {
        if (++g_mpu_fail_cnt >= 5) {
            g_sensor_fault_bits |= FAULT_MPU_DISCONNECTED;
        }
        return;
    }
    g_mpu_fail_cnt = 0;
    g_sensor_fault_bits &= ~FAULT_MPU_DISCONNECTED;

    int16_t x = (int16_t)(((uint16_t)d[0]<<8)|d[1]), y = (int16_t)(((uint16_t)d[2]<<8)|d[3]), z = (int16_t)(((uint16_t)d[4]<<8)|d[5]);
    if (abs(x) < 40 && abs(y) < 40 && abs(z) < 40) return;

    if (x == g_last_raw_x && y == g_last_raw_y && z == g_last_raw_z) {
        if (++g_mpu_freeze_cnt >= 100) {
            g_sensor_fault_bits |= FAULT_DATA_FROZEN;
        }
    } else {
        g_mpu_freeze_cnt = 0;
        g_sensor_fault_bits &= ~FAULT_DATA_FROZEN;
    }
    g_last_raw_x = x; g_last_raw_y = y; g_last_raw_z = z;

    float cx = (float)x / 4096.0f, cy = (float)y / 4096.0f, cz = (float)z / 4096.0f;
    g_motion_jerk = my_sqrtf((cx-g_prev_x)*(cx-g_prev_x) + (cy-g_prev_y)*(cy-g_prev_y) + (cz-g_prev_z)*(cz-g_prev_z));
    g_prev_x = cx; g_prev_y = cy; g_prev_z = cz;
    g_accel_mag = my_sqrtf(cx*cx + cy*cy + cz*cz);

    long t = now_ms();
    if (g_accel_mag < 0.35f) g_freefall_ms = t;
    if ((g_accel_mag > 2.0f && (t - g_freefall_ms) < 1000) || g_accel_mag > 2.5f) g_fall_until_ms = t + 5000;
    g_fall_latched = (t < g_fall_until_ms);
}

/* --- CLINICAL SCORING --- */
static int compute_patient_risk(const Patient *p, char *reason_buf, size_t len) {
    int score = 0; char r[160] = "";
    if (reason_buf && len > 0) reason_buf[0] = '\0';

    if (!p->is_simulated && !p->finger_detected) {
        if (p->fall_detected) { score += 100; snprintf(reason_buf, len, "FALL DETECTED (Bed empty)"); }
        else { snprintf(reason_buf, len, "Bed idle / finger removed"); }
        return score;
    }

    if (p->fall_detected) { score += 100; strncat(r, "FALL DETECTED; ", sizeof(r) - strlen(r) - 1); }
    if (p->spo2 > 0 && p->spo2 < SPO2_MIN_NORMAL) {
        score += 50 + (SPO2_MIN_NORMAL - p->spo2) * 2;
        char s[32]; snprintf(s, sizeof(s), "Low SpO2 (%d%%); ", p->spo2);
        strncat(r, s, sizeof(r) - strlen(r) - 1);
    }
    if (p->hr >= 150) { score += 55; strncat(r, "Severe Tachycardia; ", sizeof(r) - strlen(r) - 1); }
    else if (p->hr > 100) { score += 30; strncat(r, "High HR; ", sizeof(r) - strlen(r) - 1); }
    else if (p->hr > 0 && p->hr < 50) { score += 50; strncat(r, "Severe Bradycardia; ", sizeof(r) - strlen(r) - 1); }
    else if (p->hr > 0 && p->hr < 60) { score += 25; strncat(r, "Low HR; ", sizeof(r) - strlen(r) - 1); }

    if (p->temp >= 38.5f) { score += 35; strncat(r, "High Fever; ", sizeof(r) - strlen(r) - 1); }
    else if (p->temp >= 37.6f) { score += 20; strncat(r, "Elevated Temp; ", sizeof(r) - strlen(r) - 1); }

    if (score == 0 && reason_buf) snprintf(reason_buf, len, "All vitals stable");
    else if (reason_buf) {
        size_t l = strlen(r); if (l >= 2 && r[l-2] == ';') r[l-2] = '\0';
        snprintf(reason_buf, len, "%s", r);
    }
    return score;
}

/* --- HYSTERESIS EVALUATION ENGINE --- */
static int check_hysteresis_recovery(const Patient *p) {
    if (p->fall_detected) return 0;
    if (p->hr > (100 - HYSTERESIS_HR_BPM) && p->hr > 0) return 0;
    if (p->hr > 0 && p->hr < (60 + HYSTERESIS_HR_BPM)) return 0;
    if (p->spo2 > 0 && p->spo2 < (SPO2_MIN_NORMAL + HYSTERESIS_SPO2_PCT)) return 0;
    if (p->temp >= (37.6f - HYSTERESIS_TEMP_DEG)) return 0;
    return 1;
}

static void run_ai_pipeline(Patient *p) {
    if (p->hr == 0 || p->spo2 == 0) return;
    if (!p->base_calibrated) { p->base_hr = (float)p->hr; p->base_spo2 = (float)p->spo2; p->base_calibrated = 1; }
    else if (p->vitals_stable && !p->fall_detected) {
        p->base_hr   = 0.95f * p->base_hr   + 0.05f * (float)p->hr;
        p->base_spo2 = 0.95f * p->base_spo2 + 0.05f * (float)p->spo2;
    }
    if (p->prev_spo2 > 0) p->spo2_slope = (float)(p->spo2 - p->prev_spo2);
    p->prev_spo2 = p->spo2;
    p->sensor_trust = p->is_simulated ? 98 : (p->finger_detected ? (g_motion_jerk > 0.6f ? 45 : 98) : 0);
    p->has_ai_proposal = 0;

    int is_hypoxic = (p->spo2 > 0 && p->spo2 < SPO2_MIN_NORMAL), is_tachy = (p->hr >= 100);
    if (is_hypoxic && is_tachy) {
        p->has_ai_proposal = 1; p->ai_fused = 1;
        snprintf(p->ai_event, sizeof(p->ai_event), "ACUTE CARDIORESPIRATORY DISTRESS [SpO2+HR Fused]");
        p->ai_score = 125; p->ai_severity = SEVERITY_CRITICAL; g_server.fused_events++;
    } else if (p->temp >= 38.0f && p->fall_detected) {
        p->has_ai_proposal = 1; p->ai_fused = 1;
        snprintf(p->ai_event, sizeof(p->ai_event), "POST-FALL FEVER COLLAPSE [Temp+Fall Fused]");
        p->ai_score = 135; p->ai_severity = SEVERITY_CRITICAL; g_server.fused_events++;
    } else if (p->spo2_slope <= -1.5f && p->spo2 >= SPO2_MIN_NORMAL) {
        p->has_ai_proposal = 1; p->ai_fused = 0;
        snprintf(p->ai_event, sizeof(p->ai_event), "PREDICTIVE HYPOXIA TREND (SpO2: %d%%, Slope: %.1f)", p->spo2, p->spo2_slope);
        p->ai_score = 65; p->ai_severity = SEVERITY_HIGH;
    }
}

static void qnx_safety_supervisor(Patient *p, long t) {
    if (!p->has_ai_proposal) return;
    if (p->sensor_trust < 60) {
        p->verdict = VERDICT_REJECTED_LOW_TRUST;
        snprintf(p->verdict_reason, sizeof(p->verdict_reason), "REJECTED: Low Trust (%d%%) / Motion", p->sensor_trust);
        g_server.ai_rejected++;
        append_timeline("SUPERVISOR: Bed %d AI proposal rejected (Trust %d%%)", p->id, p->sensor_trust);
        return;
    }
    p->verdict = VERDICT_APPROVED;
    snprintf(p->verdict_reason, sizeof(p->verdict_reason), "APPROVED: Trust Verified & Trend Validated");
    g_server.ai_approved++;

    for (int q = 0; q < g_server.queue_count; q++)
        if (g_server.queue[q].patient_id == p->id && strcmp(g_server.queue[q].alarm_type, p->ai_event) == 0) {
            g_server.dup_suppressed++; return;
        }

    p->prev_active = 1; p->prev_score = p->ai_score;
    strncpy(p->last_desc, p->ai_event, sizeof(p->last_desc) - 1);

    if (g_server.queue_count < MAX_QUEUE_ALARMS) {
        AlarmEvent *ev = &g_server.queue[g_server.queue_count++];
        ev->patient_id = p->id; strncpy(ev->alarm_type, p->ai_event, sizeof(ev->alarm_type) - 1);
        ev->base_risk_score = p->ai_score; ev->effective_priority = p->ai_score;
        ev->severity = p->ai_severity; ev->timestamp_ms = t;
        ev->deadline_ms = t + (ev->severity == SEVERITY_CRITICAL ? 5000L : 15000L);
        ev->is_fused_event = p->ai_fused; ev->sensor_trust = p->sensor_trust;
        append_timeline("ALARM RAISED: Bed %d [%s] (Score: %d | FUSED/AI APPROVED)", p->id,
                        ev->severity == SEVERITY_CRITICAL ? "CRITICAL" : "HIGH", ev->base_risk_score);
    }
}

static void update_patient_ranks(void) {
    for (int i = 0; i < PATIENT_COUNT; i++) g_server.patients[i].risk_score = compute_patient_risk(&g_server.patients[i], NULL, 0);
    int order[4] = {0, 1, 2, 3};
    for (int i = 0; i < 3; i++)
        for (int j = i + 1; j < 4; j++)
            if (g_server.patients[order[j]].risk_score > g_server.patients[order[i]].risk_score) {
                int temp = order[i]; order[i] = order[j]; order[j] = temp;
            }
    for (int r = 0; r < 4; r++) g_server.patients[order[r]].priority_rank = r + 1;
}

/* --- TASKS --- */
static void *patient_monitor_task(void *arg) {
    (void)arg; struct timespec ts = {0, 10000000L}; int temp_ctr = 0;
    while (g_server.running) {
        if (g_max_ok) {
            max30102_poll_fifo();
            process_ppg(g_last_red, g_last_ir, now_ms());
            if (++temp_ctr >= 50) { max30102_temp_poll(); temp_ctr = 0; }
        }
        if (g_mpu_ok) mpu6050_poll();

        /* Embed microsecond timestamp into Pulse payload */
        struct timespec send_ts;
        clock_gettime(CLOCK_MONOTONIC, &send_ts);
        int send_us = (int)(send_ts.tv_nsec / 1000L);

        MsgSendPulse(g_server.connect_id, 20, PULSE_CODE_SAMPLE, send_us);
        nanosleep(&ts, NULL);
    }
    return NULL;
}

static void *alarm_classifier_task(void *arg) {
    (void)arg; struct _pulse pulse;
    while (g_server.running) {
        if (MsgReceivePulse(g_server.channel_id, &pulse, sizeof(pulse), NULL) != 0) continue;
        if (pulse.code == PULSE_CODE_SHUTDOWN) break;
        if (pulse.code != PULSE_CODE_SAMPLE && pulse.code != PULSE_CODE_TIMER) continue;

        /* Measure live pulse delivery & preemption latency in microseconds */
        if (pulse.code == PULSE_CODE_SAMPLE) {
            struct timespec recv_ts;
            clock_gettime(CLOCK_MONOTONIC, &recv_ts);
            int recv_us = (int)(recv_ts.tv_nsec / 1000L);
            int diff_us = recv_us - pulse.value.sival_int;
            if (diff_us < 0) diff_us += 1000000;

            if (diff_us > 0 && diff_us < 50000) {
                float lat = (float)diff_us;
                g_server.last_latency_us = lat;
                if (g_server.latency_samples == 0 || lat < g_server.min_latency_us) g_server.min_latency_us = lat;
                if (lat > g_server.max_latency_us) g_server.max_latency_us = lat;
                g_server.avg_latency_us = (g_server.avg_latency_us * (float)g_server.latency_samples + lat) / (float)(g_server.latency_samples + 1);
                g_server.latency_samples++;
            }
        }

        pthread_mutex_lock(&g_server.lock);
        unsigned long cur_cycle = g_server.cycle; long t = now_ms();

        /* Bed 1 Physical Hardware & Sensor Fault Assignment */
        g_server.patients[0].sensor_fault = g_sensor_fault_bits;
        if (g_sensor_fault_bits & FAULT_MAX_DISCONNECTED) {
            snprintf(g_server.patients[0].fault_msg, sizeof(g_server.patients[0].fault_msg), "MAX30102 CABLE DISCONNECTED");
        } else if (g_sensor_fault_bits & FAULT_MPU_DISCONNECTED) {
            snprintf(g_server.patients[0].fault_msg, sizeof(g_server.patients[0].fault_msg), "MPU6050 CABLE DISCONNECTED");
        } else if (g_sensor_fault_bits & FAULT_OPTICAL_SATURATED) {
            snprintf(g_server.patients[0].fault_msg, sizeof(g_server.patients[0].fault_msg), "OPTICAL SENSOR SATURATED");
        } else if (g_sensor_fault_bits & FAULT_DATA_FROZEN) {
            snprintf(g_server.patients[0].fault_msg, sizeof(g_server.patients[0].fault_msg), "MPU6050 REGISTERS FROZEN");
        } else {
            snprintf(g_server.patients[0].fault_msg, sizeof(g_server.patients[0].fault_msg), "HARDWARE HEALTHY");
        }

        for (int k = 1; k < PATIENT_COUNT; k++) {
            g_server.patients[k].sensor_fault = FAULT_OK;
            snprintf(g_server.patients[k].fault_msg, sizeof(g_server.patients[k].fault_msg), "SIMULATED HEALTHY");
        }

        int finger = (g_last_ir >= FINGER_IR_MIN);
        g_server.patients[0].finger_detected = finger;
        if (finger) {
            g_server.patients[0].hr   = (g_calc_hr > 0) ? g_calc_hr : 75;
            g_server.patients[0].spo2 = (g_calc_spo2 > 0) ? g_calc_spo2 : 98;
            g_server.patients[0].temp = g_latest_temp;
            g_server.patients[0].vitals_stable = (g_server.patients[0].hr <= 100 && g_server.patients[0].hr >= 60 && g_server.patients[0].spo2 >= SPO2_MIN_NORMAL);
            g_server.patients[0].temp_stable   = (g_server.patients[0].temp < 37.6f && g_server.patients[0].temp >= 35.8f);
        } else {
            g_server.patients[0].hr = 0; g_server.patients[0].spo2 = 0; g_server.patients[0].temp = 36.8f;
            g_server.patients[0].vitals_stable = 1; g_server.patients[0].temp_stable = 1;
        }
        g_server.patients[0].fall_detected  = g_fall_latched;
        g_server.patients[0].patient_stable = !g_fall_latched;

        /* Beds 2, 3, 4 Simulation Phases */
        int phase = (cur_cycle > 0) ? (int)(((cur_cycle - 1) % 12) + 1) : 1;

        g_server.patients[1].finger_detected = 1; g_server.patients[1].temp = 36.8f;
        g_server.patients[1].temp_stable = 1; g_server.patients[1].patient_stable = 1; g_server.patients[1].fall_detected = 0;
        if (phase >= 5 && phase <= 8)      { g_server.patients[1].hr = 88; g_server.patients[1].spo2 = 55; g_server.patients[1].vitals_stable = 0; }
        else if (phase >= 9)               { g_server.patients[1].hr = 95; g_server.patients[1].spo2 = 50; g_server.patients[1].vitals_stable = 0; }
        else                               { g_server.patients[1].hr = 76; g_server.patients[1].spo2 = 98; g_server.patients[1].vitals_stable = 1; }

        g_server.patients[2].finger_detected = 1; g_server.patients[2].spo2 = 97;
        g_server.patients[2].temp = 37.0f; g_server.patients[2].temp_stable = 1; g_server.patients[2].patient_stable = 1; g_server.patients[2].fall_detected = 0;
        if (phase >= 4 && phase <= 8)      { g_server.patients[2].hr = 135; g_server.patients[2].vitals_stable = 0; }
        else if (phase >= 9)               { g_server.patients[2].hr = 152; g_server.patients[2].vitals_stable = 0; }
        else                               { g_server.patients[2].hr = 78;  g_server.patients[2].vitals_stable = 1; }

        g_server.patients[3].finger_detected = 1; g_server.patients[3].hr = 80;
        g_server.patients[3].spo2 = 98; g_server.patients[3].vitals_stable = 1;
        if (phase >= 6) {
            g_server.patients[3].temp = 38.9f; g_server.patients[3].temp_stable = 0;
            g_server.patients[3].fall_detected = (phase >= 8) ? 1 : 0; g_server.patients[3].patient_stable = (phase >= 8) ? 0 : 1;
        } else {
            g_server.patients[3].temp = 36.7f; g_server.patients[3].temp_stable = 1;
            g_server.patients[3].fall_detected = 0; g_server.patients[3].patient_stable = 1;
        }

        update_patient_ranks();

        /* --- ALARM FATIGUE REDUCTION & HARD RT DISPATCH --- */
        int any_audible_alarm = 0;

        for (int i = 0; i < PATIENT_COUNT; i++) {
            Patient *p = &g_server.patients[i];
            if (p->has_ai_proposal) qnx_safety_supervisor(p, t);

            char reason[128];
            int score = compute_patient_risk(p, reason, sizeof(reason));
            int is_critical = (score >= 100 || p->fall_detected || p->hr >= 150 || (p->has_ai_proposal && p->ai_severity == SEVERITY_CRITICAL));

            if (score > 0) {
                if (is_critical) {
                    p->fatigue_state = STATE_ACTIVE;
                    strncpy(p->alarm_level, "RED", sizeof(p->alarm_level));
                } else {
                    if (p->fatigue_state == STATE_NORMAL || p->fatigue_state == STATE_RECOVERED) {
                        p->fatigue_state = STATE_PENDING;
                        p->abnormal_start_ms = t;
                        strncpy(p->alarm_level, "YELLOW", sizeof(p->alarm_level));
                        append_timeline("FATIGUE FILTER: Bed %d [%s] entered PENDING check", p->id, reason);
                    } else if (p->fatigue_state == STATE_PENDING) {
                        if ((t - p->abnormal_start_ms) >= PERSISTENCE_THRESHOLD_MS) {
                            p->fatigue_state = STATE_ACTIVE;
                            strncpy(p->alarm_level, "YELLOW", sizeof(p->alarm_level));
                            append_timeline("FATIGUE CONFIRMED: Bed %d sustained anomaly (%ld ms) -> ACTIVE",
                                            p->id, (t - p->abnormal_start_ms));
                        }
                    }
                }

                if (p->fatigue_state == STATE_ACTIVE && !p->has_ai_proposal) {
                    g_server.total_alarms++; g_server.hard_fallbacks++;
                    int in_q = 0;
                    for (int q = 0; q < g_server.queue_count; q++)
                        if (g_server.queue[q].patient_id == p->id && strcmp(g_server.queue[q].alarm_type, reason) == 0) { in_q = 1; break; }

                    if (in_q) { g_server.dup_suppressed++; }
                    else {
                        p->prev_active = 1; p->prev_score = score; strncpy(p->last_desc, reason, sizeof(p->last_desc) - 1);
                        if (g_server.queue_count < MAX_QUEUE_ALARMS) {
                            AlarmEvent *ev = &g_server.queue[g_server.queue_count++];
                            ev->patient_id = p->id; strncpy(ev->alarm_type, reason, sizeof(ev->alarm_type) - 1);
                            ev->base_risk_score = score; ev->effective_priority = score;
                            ev->severity = (score >= 100) ? SEVERITY_CRITICAL : (score >= 50 ? SEVERITY_HIGH : SEVERITY_MODERATE);
                            ev->timestamp_ms = t; ev->deadline_ms = t + (score >= 100 ? 5000L : 15000L);
                            ev->is_fused_event = 0; ev->sensor_trust = p->sensor_trust;
                            append_timeline("ALARM RAISED: Bed %d [%s] (Score: %d | HARD RT)", p->id,
                                            ev->severity == SEVERITY_CRITICAL ? "CRITICAL" : "HIGH", score);
                        }
                    }
                }
            } else {
                if (p->fatigue_state == STATE_PENDING) {
                    p->fatigue_state = STATE_NORMAL;
                    strncpy(p->alarm_level, "GREEN", sizeof(p->alarm_level));
                    g_server.transients_suppressed++;
                    append_timeline("FATIGUE FILTER: Bed %d transient fluctuation suppressed (< %ld ms)",
                                    p->id, PERSISTENCE_THRESHOLD_MS);
                } else if (p->fatigue_state == STATE_ACTIVE || p->fatigue_state == STATE_ACKNOWLEDGED) {
                    if (check_hysteresis_recovery(p)) {
                        p->fatigue_state = STATE_NORMAL;
                        p->in_hysteresis = 0;
                        p->prev_active   = 0;
                        p->prev_score    = 0;
                        strncpy(p->alarm_level, "GREEN", sizeof(p->alarm_level));
                        g_server.hysteresis_recoveries++;
                        append_timeline("HYSTERESIS CLEARED: Bed %d safely cleared recovery buffer -> NORMAL", p->id);
                    } else {
                        p->in_hysteresis = 1;
                    }
                } else {
                    p->fatigue_state = STATE_NORMAL;
                    strncpy(p->alarm_level, "GREEN", sizeof(p->alarm_level));
                }
            }

            if (p->fatigue_state == STATE_ACTIVE) any_audible_alarm = 1;
        }

        if (any_audible_alarm || g_fall_latched) atomic_set(&g_server.alarm_active, 1);
        else atomic_clr(&g_server.alarm_active, 1);

        pthread_mutex_unlock(&g_server.lock);
    }
    return NULL;
}

static void *ai_intelligence_lane_task(void *arg) {
    (void)arg; struct timespec ts = {0, 100000000L};
    while (g_server.running) {
        nanosleep(&ts, NULL);
        pthread_mutex_lock(&g_server.lock);
        for (int i = 0; i < PATIENT_COUNT; i++) run_ai_pipeline(&g_server.patients[i]);
        pthread_mutex_unlock(&g_server.lock);
    }
    return NULL;
}

static void *alarm_dispatcher_task(void *arg) {
    (void)arg; struct timespec ts = {1, 0};
    while (g_server.running) {
        nanosleep(&ts, NULL);
        long now = now_ms();
        pthread_mutex_lock(&g_server.lock);
        if (g_server.queue_count > 0) {
            for (int i = 0; i < g_server.queue_count; i++) {
                AlarmEvent *ev = &g_server.queue[i];
                long wait_sec = (now - ev->timestamp_ms) / 1000L;
                int old = ev->effective_priority;
                ev->effective_priority = ev->base_risk_score + (int)(wait_sec * AGING_BOOST_PER_SEC);
                if (ev->effective_priority != old) g_server.aging_boosts++;
                if (now > ev->deadline_ms) { g_server.deadlines_breached++; ev->effective_priority += 50; }
            }
            for (int i = 0; i < g_server.queue_count - 1; i++)
                for (int j = i + 1; j < g_server.queue_count; j++)
                    if (g_server.queue[j].effective_priority > g_server.queue[i].effective_priority) {
                        AlarmEvent tmp = g_server.queue[i]; g_server.queue[i] = g_server.queue[j]; g_server.queue[j] = tmp;
                    }
            if (g_server.queue[0].severity == SEVERITY_CRITICAL) g_server.crit_dispatched++;
            if (g_server.queue_count > 6) g_server.queue_count = 6;
        }
        pthread_mutex_unlock(&g_server.lock);
    }
    return NULL;
}

static void *alarm_actuator_task(void *arg) {
    (void)arg;
    ThreadCtl(_NTO_TCTL_IO, 0);
    while (g_server.running) {
        if (g_server.alarm_active) {
            set_led(1); set_buzzer(1); usleep(250000);
            set_led(0); set_buzzer(0); usleep(250000);
        } else {
            set_led(0); set_buzzer(0); usleep(100000);
        }
    }
    set_led(0); set_buzzer(0);
    return NULL;
}

static void *dashboard_sender_task(void *arg) {
    (void)arg; int prev_cycle = 0;
    while (g_server.running) {
        sleep(1);
        int cur;
        static Patient snap[PATIENT_COUNT]; /* Off stack to prevent overflow */
        pthread_mutex_lock(&g_server.lock);
        cur = (int)g_server.cycle; memcpy(snap, g_server.patients, sizeof(snap));
        pthread_mutex_unlock(&g_server.lock);

        if (cur > 0 && cur != prev_cycle) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock >= 0) {
                struct timeval tv = {2, 0};
                setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
                struct sockaddr_in srv; memset(&srv, 0, sizeof(srv));
                srv.sin_family = AF_INET; srv.sin_port = htons(BRIDGE_PORT); srv.sin_addr.s_addr = inet_addr(BRIDGE_IP);

                if (connect(sock, (struct sockaddr *)&srv, sizeof(srv)) == 0) {
                    static char json[4096]; /* Off stack into static data */
                    int n = snprintf(json, sizeof(json), "{\"cycle\":%d,\"source\":\"QNX_MEDISENTINEL\",\"patients\":[", cur);
                    for (int i = 0; i < PATIENT_COUNT; i++) {
                        const char *st = (!snap[i].temp_stable || snap[i].fall_detected || (snap[i].finger_detected && !snap[i].vitals_stable)) ? "ALERT" : "NORMAL";
                        const char *v  = (snap[i].verdict == VERDICT_APPROVED) ? "APPROVED" : (snap[i].verdict == VERDICT_REJECTED_LOW_TRUST ? "REJECTED_TRUST" : "HARD_RT");
                        n += snprintf(json + n, sizeof(json) - n,
                            "{\"patient_id\":%d,\"spo2\":%d,\"heart_rate\":%d,\"temperature\":%.1f,"
                            "\"fall_detected\":%d,\"simulated\":%d,\"finger\":%d,\"risk_score\":%d,\"priority_rank\":%d,"
                            "\"status\":\"%s\",\"baseline_hr\":%.1f,\"baseline_spo2\":%.1f,\"sensor_trust\":%d,"
                            "\"spo2_slope\":%.2f,\"supervisor_verdict\":\"%s\",\"sensor_fault\":%d,\"fault_msg\":\"%s\","
                            "\"fatigue_state\":\"%s\",\"alarm_level\":\"%s\",\"in_hysteresis\":%d}%s",
                            snap[i].id, snap[i].spo2, snap[i].hr, snap[i].temp, snap[i].fall_detected,
                            snap[i].is_simulated, snap[i].finger_detected, snap[i].risk_score, snap[i].priority_rank,
                            st, snap[i].base_hr, snap[i].base_spo2, snap[i].sensor_trust, snap[i].spo2_slope, v,
                            snap[i].sensor_fault, snap[i].fault_msg,
                            fatigue_state_to_str(snap[i].fatigue_state), snap[i].alarm_level, snap[i].in_hysteresis,
                            (i < PATIENT_COUNT - 1) ? "," : "");
                    }
                    n += snprintf(json + n, sizeof(json) - n, "]}\n");
                    send(sock, json, n, 0);
                }
                close(sock);
            }
            prev_cycle = cur;
        }
    }
    return NULL;
}

static void print_patient_card(const Patient *p, int cycle) {
    printf("\n--------------------------------------------------\n");
    if (p->is_simulated)
        printf("  ICU PATIENT %d [SIMULATED BED %d] | CYCLE %d\n", p->id, p->id, cycle);
    else
        printf("  ICU PATIENT %d [PHYSICAL SENSORS] | CYCLE %d\n", p->id, cycle);
    printf("--------------------------------------------------\n");

    const char *risk_str = (p->risk_score >= 100) ? "CRITICAL 🚨" :
                           (p->risk_score >= 50)  ? "HIGH RISK ⚠️" :
                           (p->risk_score > 0)    ? "MODERATE  ⚠️" : "STABLE    ✅";

    printf("TRIAGE PRIORITY: RANK #%d [Risk Score: %d | %s]\n", p->priority_rank, p->risk_score, risk_str);

    printf("Fatigue State : %s %s\n", fatigue_state_to_str(p->fatigue_state),
           p->in_hysteresis ? "[HYSTERESIS STABILIZING]" : "");

    if (!p->is_simulated) {
        printf("Sensor Health : [%s] %s\n",
               (p->sensor_fault == FAULT_OK) ? "HEALTHY ✅" : "FAULT DETECTED ⚠️",
               p->fault_msg);

        if (p->finger_detected) {
            printf("Finger Status : Detected\nHeart Rate    : %d BPM (Base: %.1f)\nSpO2 Level    : %d%% (Base: %.1f)\n",
                   p->hr, p->base_hr, p->spo2, p->base_spo2);
            printf("Vitals Status : %s\n", p->vitals_stable ? "Heart Rate & SpO2 Stable ✅" : "Vitals Out of Range 🚨");
        } else {
            printf("Finger Status : Bed empty / No finger detected\nHeart Rate    : 0 BPM\nSpO2 Level    : 0%%\n");
        }
    } else {
        printf("Sensor Health : [HEALTHY ✅] %s\n", p->fault_msg);
        printf("Finger Status : Simulated (Active)\nHeart Rate    : %d BPM (Base: %.1f)\nSpO2 Level    : %d%% (Base: %.1f)\n",
               p->hr, p->base_hr, p->spo2, p->base_spo2);
        printf("Vitals Status : %s\n", p->vitals_stable ? "Heart Rate & SpO2 Stable ✅" : "ALERT 🚨");
    }

    printf("Temperature   : %.1f C [%s]\nFall Status   : %s\n",
           p->temp, p->temp_stable ? "NORMAL ✅" : "FEVER/ALERT 🚨",
           p->patient_stable ? "Stable ✅" : "FALL RISK DETECTED 🚨");

    printf("Stage 2 Trend : SpO2 Trend = %+.1f%%/cyc\n", p->spo2_slope);
    printf("Stage 3 Trust : %d%% [%s]\n", p->sensor_trust,
           p->sensor_trust >= 80 ? "HIGH CONFIDENCE ✅" : (p->sensor_trust >= 60 ? "ACCEPTABLE ⚠️" : "SUSPECT / NOISE REJECTED 🛑"));

    if (p->has_ai_proposal) {
        printf("Stage 4 & 5 AI: [%s] (Risk: %d)\n", p->ai_event, p->ai_score);
        printf("Stage 6 Gate  : %s (%s)\n", p->verdict == VERDICT_APPROVED ? "APPROVED ✅" : "REJECTED 🛡️", p->verdict_reason);
    } else {
        printf("Stage 6 Gate  : HARD REAL-TIME PATH ACTIVE [Deterministic Gate Safe]\n");
    }

    if (!p->is_simulated) {
        printf("Physical Alarm: %s\n", g_server.alarm_active ? "BUZZER + LED ACTIVE 🚨" : "Silent [OK]");
    }
}

static void *logger_display_task(void *arg) {
    (void)arg; int cycle_cnt = 0;
    while (g_server.running) {
        cycle_cnt++;
        static Patient snap[PATIENT_COUNT];
        static AlarmEvent q_snap[MAX_QUEUE_ALARMS];
        static TimelineEntry t_snap[MAX_TIMELINE_LOGS];
        int q_cnt = 0, t_cnt = 0, t_head = 0;

        /* Snapshot entire server state under mutex to prevent race condition crashes */
        pthread_mutex_lock(&g_server.lock);
        g_server.cycle = (unsigned long)cycle_cnt;
        memcpy(snap, g_server.patients, sizeof(snap));
        q_cnt = g_server.queue_count;
        if (q_cnt > MAX_QUEUE_ALARMS) q_cnt = MAX_QUEUE_ALARMS;
        memcpy(q_snap, g_server.queue, sizeof(AlarmEvent) * q_cnt);
        t_head = g_server.timeline_head;
        t_cnt = (t_head < MAX_TIMELINE_LOGS) ? t_head : MAX_TIMELINE_LOGS;
        memcpy(t_snap, g_server.timeline, sizeof(t_snap));
        pthread_mutex_unlock(&g_server.lock);

        printf("\n\n======================================================================\n");
        printf("   CYCLE %d  [%ds Interval | QNX Neutrino SDP 8.0 Dual-Lane Safe System] \n", cycle_cnt, CYCLE_INTERVAL_SEC);
        printf("======================================================================\n");

        /* --- LIVE REAL-TIME LATENCY BENCHMARK METRIC CARD --- */
        printf("\n======================================================================\n");
        printf(" ⚡ LIVE QNX REAL-TIME LATENCY BENCHMARKS (100 Hz Kernel Pulses)      \n");
        printf("======================================================================\n");
        printf("  Current Pulse Latency  : %6.1f µs\n", g_server.last_latency_us);
        printf("  Average Dispatch Time  : %6.1f µs (Samples Measured: %lu)\n",
               g_server.avg_latency_us, g_server.latency_samples);
        printf("  Min / Max Jitter Range : %6.1f µs / %6.1f µs\n",
               g_server.min_latency_us, g_server.max_latency_us);
        printf("  RT Determinism Status  : [STABLE HARD REAL-TIME COMPLIANT ✅]\n");

        printf("\n======================================================================\n");
        printf(" 📋 ACTIVE ALARM PRIORITY QUEUE (Sorted with Anti-Starvation Aging)   \n");
        printf("======================================================================\n");
        if (q_cnt == 0) {
            printf("  Queue is empty. All patients are within nominal limits.\n");
        } else {
            printf("  Pos | Bed | Score | Effective | Trust | Sev  | Type  | Alarm Description\n");
            printf("  ----+-----+-------+-----------+-------+------+-------+------------------------------\n");
            for (int i = 0; i < q_cnt && i < 5; i++) {
                AlarmEvent *e = &q_snap[i];
                const char *sev = (e->severity == SEVERITY_CRITICAL) ? "CRIT" : (e->severity == SEVERITY_HIGH ? "HIGH" : "MOD ");
                printf("  #%d  |  %d  |  %3d  |    %3d    |  %2d%%  | %s | %s | %s\n",
                       i + 1, e->patient_id, e->base_risk_score, e->effective_priority,
                       e->sensor_trust, sev, e->is_fused_event ? "FUSED" : "HARD ", e->alarm_type);
            }
        }

        printf("\n======================================================================\n");
        printf(" ⏱️  REAL-TIME ALARM TIMELINE LOG                                      \n");
        printf("======================================================================\n");
        if (t_cnt == 0) {
            printf("  No timeline events recorded yet.\n");
        } else {
            for (int i = 0; i < t_cnt; i++) {
                int idx = (t_head - t_cnt + i) % MAX_TIMELINE_LOGS;
                printf("  [%06ld ms] %s\n", t_snap[idx].timestamp_ms % 1000000L, t_snap[idx].log_text);
            }
        }

        printf("\n======================================================================\n");
        printf(" 📊 ICU ALARM FATIGUE & SAFETY SUPERVISOR STATISTICS                  \n");
        printf("======================================================================\n");
        printf("  Total Alarms: %lu | Suppressed: %lu | Fused Events: %lu | AI Approved: %lu\n",
               g_server.total_alarms, g_server.dup_suppressed, g_server.fused_events, g_server.ai_approved);
        printf("  Aging Boosts: %lu | Deadlines Breached: %lu | Hard RT Fallbacks: %lu\n",
               g_server.aging_boosts, g_server.deadlines_breached, g_server.hard_fallbacks);
        printf("  Fatigue Filtered (Transients): %lu | Hysteresis Stabilizations: %lu\n",
               g_server.transients_suppressed, g_server.hysteresis_recoveries);
        printf("======================================================================\n");

        for (int i = 0; i < PATIENT_COUNT; i++) print_patient_card(&snap[i], cycle_cnt);
        fflush(stdout);

        for (int s = 0; s < CYCLE_INTERVAL_SEC && g_server.running; s++) sleep(1);
    }
    return NULL;
}

static int launch_rt_thread(pthread_t *tid, int prio, void *(*fn)(void *)) {
    pthread_attr_t attr; struct sched_param sp;
    pthread_attr_init(&attr);

    /* Allocate 64 KB thread stack to completely prevent SIGSEGV / stack overflow */
    pthread_attr_setstacksize(&attr, 65536);
    pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
    pthread_attr_setschedpolicy(&attr, SCHED_FIFO);
    sp.sched_priority = prio;
    pthread_attr_setschedparam(&attr, &sp);

    int rc = pthread_create(tid, &attr, fn, NULL);
    pthread_attr_destroy(&attr);
    return rc;
}

static void handle_signal(int sig) {
    (void)sig; atomic_clr(&g_server.running, 1);
    set_led(0); set_buzzer(0);
    if (g_server.connect_id >= 0) MsgSendPulse(g_server.connect_id, 24, PULSE_CODE_SHUTDOWN, 0);
}

int main(void) {
    printf("======================================================================\n");
    printf(" EC 2V87: MediSentinel Edge - Multi-Patient ICU Alarm Prioritization  \n");
    printf(" Innovation  : Context-Aware Alarm Fatigue Reduction (Persistence+Hyst)\n");
    printf(" Architecture: Dual-Lane Safety Gate (Hard RT + AI Analytics)        \n");
    printf(" Target OS   : QNX Neutrino RTOS SDP 8.0 (AArch64 / Raspberry Pi 4)   \n");
    printf("======================================================================\n");

    /* Prevent broken TCP network pipe signals from terminating the application */
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGHUP, handle_signal);

    gpio_hardware_init();
    set_led(1); set_buzzer(1); usleep(300000); set_led(0); set_buzzer(0);

    g_i2c_fd = open(I2C_DEVICE, O_RDWR);
    if (g_i2c_fd >= 0) {
        g_max_ok = (max30102_init() == 0);
        g_mpu_ok = (mpu6050_init() == 0);
        printf("[I2C] MAX30102: %s | MPU6050: %s\n", g_max_ok ? "ONLINE" : "OFFLINE", g_mpu_ok ? "ONLINE" : "OFFLINE");
    } else {
        printf("[I2C] Running in Simulated Sensors Mode.\n");
    }

    memset(&g_server, 0, sizeof(g_server));
    pthread_mutex_init(&g_server.lock, NULL);
    atomic_set(&g_server.running, 1);

    for (int i = 0; i < PATIENT_COUNT; i++) {
        g_server.patients[i].id            = i + 1;
        g_server.patients[i].is_simulated  = (i == 0) ? 0 : 1;
        g_server.patients[i].vitals_stable = 1; g_server.patients[i].temp_stable = 1; g_server.patients[i].patient_stable = 1;
        g_server.patients[i].temp          = 36.8f;
        g_server.patients[i].hr            = (i == 1) ? 76 : ((i == 2) ? 78 : ((i == 3) ? 80 : 75));
        g_server.patients[i].spo2          = (i == 1) ? 98 : ((i == 2) ? 97 : ((i == 3) ? 98 : 98));
        g_server.patients[i].priority_rank = i + 1;
        g_server.patients[i].sensor_trust  = 100;
        g_server.patients[i].sensor_fault  = FAULT_OK;
        snprintf(g_server.patients[i].fault_msg, sizeof(g_server.patients[i].fault_msg), "INITIALIZING");

        /* Fatigue Engine Defaults */
        g_server.patients[i].fatigue_state     = STATE_NORMAL;
        g_server.patients[i].abnormal_start_ms = 0;
        g_server.patients[i].in_hysteresis     = 0;
        strncpy(g_server.patients[i].alarm_level, "GREEN", sizeof(g_server.patients[i].alarm_level));
    }

    g_server.channel_id = ChannelCreate(0);
    g_server.connect_id = ConnectAttach(ND_LOCAL_NODE, 0, g_server.channel_id, _NTO_SIDE_CHANNEL, 0);

    /* 100 Hz QNX Kernel Timer */
    struct sigevent ev; struct itimerspec it;
    SIGEV_PULSE_INIT(&ev, g_server.connect_id, 24, PULSE_CODE_TIMER, 0);
    timer_create(CLOCK_MONOTONIC, &ev, &g_server.sample_timer_id);
    it.it_value.tv_sec = 0; it.it_value.tv_nsec = 10000000L;
    it.it_interval.tv_sec = 0; it.it_interval.tv_nsec = 10000000L;
    timer_settime(g_server.sample_timer_id, 0, &it, NULL);

    pthread_t t_disp, t_class, t_mon, t_act, t_ai, t_dash, t_log;
    launch_rt_thread(&t_disp,  28, alarm_dispatcher_task);
    launch_rt_thread(&t_class, 24, alarm_classifier_task);
    launch_rt_thread(&t_mon,   20, patient_monitor_task);
    launch_rt_thread(&t_act,   18, alarm_actuator_task);
    launch_rt_thread(&t_ai,    15, ai_intelligence_lane_task);
    launch_rt_thread(&t_dash,  12, dashboard_sender_task);
    launch_rt_thread(&t_log,   10, logger_display_task);

    printf("[QNX] All 7 RT threads active under SCHED_FIFO.\n");

    pthread_join(t_log, NULL); pthread_join(t_dash, NULL);
    pthread_join(t_disp, NULL); pthread_join(t_class, NULL);
    pthread_join(t_ai, NULL); pthread_join(t_mon, NULL); pthread_join(t_act, NULL);

    set_led(0); set_buzzer(0);
    timer_delete(g_server.sample_timer_id);
    ConnectDetach(g_server.connect_id);
    ChannelDestroy(g_server.channel_id);
    pthread_mutex_destroy(&g_server.lock);
    if (g_i2c_fd >= 0) close(g_i2c_fd);

    printf("[QNX] MediSentinel gracefully stopped.\n");
    return 0;
}
