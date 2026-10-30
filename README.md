# Medisentinal-edge
# QNX MediSentinel
### Predictive Clinical-Criticality & Alarm Fatigue Orchestration System

**Team:** QubeX  
**Problem Statement:** #19 – ICU Multi-Patient Alarm Prioritization System  
**Target Platform:** Raspberry Pi 4 (AArch64)  
**Operating System:** QNX SDP 8.0 – QNX Neutrino RTOS

---

## 1. Project Overview

QNX MediSentinel is a real-time ICU patient monitoring and intelligent alarm prioritization system designed to reduce alarm fatigue in multi-patient intensive care environments.

In an ICU, multiple patients are continuously monitored using sensors that generate alarms whenever abnormal conditions are detected. However, frequent false, duplicate, and non-actionable alarms can overwhelm healthcare professionals, increasing the possibility of missing critical events.

MediSentinel addresses this problem through a dual-lane architecture that separates hard real-time alarm handling from intelligent patient analysis.

The system monitors four patients, processes vital signs, evaluates abnormal conditions, identifies sensor faults, and prioritizes alarms based on clinical criticality.

A Python communication bridge connects the QNX application to a web-based dashboard, where patient information and alarm status are displayed in real time.

**Core principle: AI or analytics failure must never block the hard real-time alarm path.**

---

## 2. Problem Statement

Traditional ICU monitoring systems may generate multiple alarms for the same patient due to:

- Repeated abnormal readings.
- False alarms caused by sensor disturbances.
- Duplicate alarms from related vital-sign abnormalities.
- Temporary fluctuations in physiological measurements.
- Lack of patient-specific baseline analysis.
- Excessive non-critical notifications.

These conditions contribute to alarm fatigue and can delay responses to genuinely critical events.

### Proposed Solution

QNX MediSentinel introduces:

1. Continuous multi-patient vital-sign monitoring.
2. Independent hard real-time alarm processing.
3. Patient-specific baseline monitoring.
4. Trend-based deterioration analysis.
5. Sensor reliability and fault detection.
6. Alarm persistence filtering and hysteresis.
7. Clinical event prioritization.
8. A centralized web dashboard.

---

## 3. System Architecture

The system consists of three major components:

### A. QNX C Application

The QNX application is the core real-time monitoring system.

Responsibilities:

- Acquires sensor data.
- Monitors four ICU patients.
- Processes real-time alarms.
- Performs patient baseline analysis.
- Evaluates vital-sign trends.
- Detects sensor faults.
- Maintains alarm states.
- Communicates patient information through TCP sockets.

The application uses QNX/POSIX facilities such as:

- Threads and scheduling.
- `SCHED_FIFO` real-time scheduling.
- QNX channels and pulses.
- Timers.
- Mutexes.
- Atomic variables.
- I2C communication.
- GPIO memory-mapped I/O.

### B. Python Edge Bridge

The Python bridge acts as the communication layer between the QNX application and the web dashboard.

Responsibilities:

- Receives patient data from the QNX application through TCP.
- Parses incoming JSON messages.
- Validates the received data structure.
- Maintains the latest patient information.
- Provides an HTTP endpoint for the dashboard.
- Serves the dashboard's HTML, CSS, and JavaScript files.

### C. Web-Based Dashboard

The dashboard provides a centralized interface for monitoring four patients.

It displays:

- Heart rate.
- SpO₂.
- Temperature.
- Fall detection.
- ECG and plethysmography waveforms.
- Patient risk scores.
- Alarm priority.
- Sensor trust.
- Vital-sign trends.
- Alarm fatigue states.
- Alarm acknowledgement controls.
- System status and performance indicators.

The dashboard supports both live QNX data and simulated demonstration data.

---

## 4. Dual-Lane Real-Time Architecture

The most important feature of MediSentinel is its dual-lane architecture.

The processing system is divided into two independent lanes.

### Lane 1: Hard Real-Time Alarm Lane

This lane handles critical alarm processing.

Functions:

- Receives vital-sign information.
- Executes hard alarm classification.
- Identifies critical conditions.
- Dispatches alarms immediately.

This lane is designed to operate independently of the analytics lane.

### Lane 2: AI Intelligence and Analytics Lane

This lane performs additional patient analysis.

Functions:

- Patient-specific baseline calculation.
- Deterioration trend prediction.
- Sensor trust evaluation.
- Clinical event fusion.
- Criticality scoring.
- Alarm prioritization support.

The analytics lane uses rule-based analysis and statistical processing. It is not currently a trained machine-learning model.

### Architecture Flow

```text
             ICU PATIENTS (1–4)
                     |
              VITAL ACQUISITION
                     |
             +-------+-------+
             |               |
             v               v
       HARD REAL-TIME    AI INTELLIGENCE
           LANE               LANE
             |                 |
      Hard Alarm          Patient Baseline
      Classification           |
             |            Trend Prediction
      Alarm Dispatcher         |
             |            Sensor Trust
             |                 |
             |            Event Fusion
             |                 |
             |          Criticality Score
             |                 |
             +--------+--------+
                      |
               QNX APPLICATION
                      |
                TCP / JSON
                      |
               PYTHON BRIDGE
                      |
                 HTTP API
                      |
               WEB DASHBOARD
```

**Design principle:** Failure or delay in the analytics lane must not prevent the hard real-time alarm lane from processing critical alarms.

---

## 5. Alarm Fatigue Reduction

MediSentinel uses an alarm fatigue management mechanism to reduce repeated and unnecessary notifications.

### 5.1 Persistence Filtering

An abnormal reading is evaluated over a persistence period before being treated as a sustained event.

The current configured persistence threshold is:

`3000 milliseconds (3 seconds)`

This helps avoid triggering repeated alarms due to short-lived fluctuations.

### 5.2 Hysteresis

Hysteresis guard bands help prevent alarms from repeatedly switching between normal and abnormal states when readings fluctuate near a threshold.

### 5.3 Alarm State Machine

The system uses an extended finite state machine with the following states:

| State | Description |
|---|---|
| NORMAL | Patient readings are within configured limits. |
| PENDING | An abnormal condition is being evaluated. |
| ACTIVE | An alarm condition has been confirmed. |
| ACKNOWLEDGED | The alarm has been acknowledged. |
| RECOVERED | The patient reading has returned to the recovery condition. |

### 5.4 Queue Aging

Queue aging helps manage waiting alarms by considering how long they have remained in the alarm queue.

### 5.5 Sensor Fault Detection

Sensor plausibility and consistency checks help identify unreliable readings and possible sensor faults.

---

## 6. Patient Monitoring

The system supports four patient profiles.

| Patient | Monitoring Mode |
|---|---|
| Patient 1 | Physical sensor monitoring |
| Patient 2 | Simulated patient data |
| Patient 3 | Simulated patient data |
| Patient 4 | Simulated patient data |

### Physical Sensors

The hardware interface includes:

| Sensor | I2C Address | Purpose |
|---|---|---|
| MAX30102 | 0x57 | Heart rate and SpO₂ monitoring |
| MPU6050 | 0x68 | Motion and fall-related monitoring |

**I2C Device:** `/dev/i2c1`

The system also uses GPIO outputs for local indication.

| GPIO | Purpose |
|---|---|
| GPIO 17 | LED indication |
| GPIO 4 | Buzzer indication |

**GPIO Base Address:** `0xFE200000` (BCM2711)

---

## 7. Technologies Used

| Technology | Purpose |
|---|---|
| C | Core embedded application |
| QNX Neutrino RTOS | Real-time operating system |
| QNX SDP 8.0 | Development environment |
| Raspberry Pi 4 | Target hardware |
| POSIX Threads | Concurrent processing |
| SCHED_FIFO | Real-time thread scheduling |
| QNX Channels and Pulses | Inter-thread communication |
| I2C | Sensor communication |
| GPIO | LED and buzzer control |
| TCP Sockets | QNX-to-Python communication |
| JSON | Data exchange format |
| Python | Communication bridge |
| HTML | Dashboard structure |
| CSS | Dashboard styling |
| JavaScript | Dashboard logic and visualization |

---

## 8. Python Communication Bridge

The Python bridge connects the QNX application with the dashboard.

It runs two servers:

### TCP Server

- Port: `5000`
- Receives JSON data from the QNX application.
- Processes newline-delimited JSON messages.
- Checks that the received JSON root is an object.
- Checks that the `patients` field is a list containing exactly four patient entries.
- Stores the latest received data using a thread lock.
- Returns `OK` or `ERROR` as an acknowledgement.

### HTTP Server

- Port: `8000`
- Serves the dashboard files.
- Provides the `/api/data` endpoint.
