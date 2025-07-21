/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2025 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#ifndef FLB_FILTER_LOOKUP_H
#define FLB_FILTER_LOOKUP_H

#include <fluent-bit/flb_filter_plugin.h>
#include <fluent-bit/flb_hash_table.h>
#include <fluent-bit/flb_record_accessor.h>
#include <fluent-bit/flb_metrics.h>
#include <fluent-bit/flb_scheduler.h>
#include <monkey/mk_core/mk_list.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>

/* Metric constants */
#define FLB_LOOKUP_METRIC_PROCESSED     200
#define FLB_LOOKUP_METRIC_MATCHED       201
#define FLB_LOOKUP_METRIC_SKIPPED       202
#define FLB_LOOKUP_METRIC_REFRESH       203
#define FLB_LOOKUP_METRIC_REFRESH_ERRORS 204

/* Refresh states - SIMPLIFIED */
enum refresh_state {
    REFRESH_NORMAL,     /* Normal operation - use refresh_interval */
    REFRESH_RETRY       /* Failed refresh - use retry_interval (faster) */
};

struct lookup_ctx {
    struct flb_filter_instance *ins;
    char *file;
    char *lookup_key;
    char *result_key;
    char *delimiter;
    struct flb_hash_table *ht;
    struct flb_record_accessor *ra_lookup_key;
    int ignore_case;
    struct mk_list val_list;
    
    /* CSV refresh configuration - SIMPLIFIED */
    int refresh_interval;                /* Normal refresh interval in seconds (e.g., 86400 for daily) */
    int retry_interval;                  /* Retry interval after failure in seconds (e.g., 600 for 10 min) */
    
    /* Refresh state tracking - SIMPLIFIED */
    enum refresh_state circuit_state;    /* NORMAL or RETRY mode */
    int refresh_failures;               /* Count of consecutive failures (for logging/metrics) */
    time_t last_refresh;                /* Last attempted refresh timestamp */
    time_t last_successful_refresh;     /* Last successful refresh timestamp */
    struct flb_sched_timer *refresh_timer; /* Scheduler timer handle */
    pthread_rwlock_t refresh_rwlock;    /* RW lock for CSV reloading - readers: filter, writer: refresh */
    
#ifdef FLB_HAVE_METRICS
    struct cmt_counter *cmt_processed;
    struct cmt_counter *cmt_matched;
    struct cmt_counter *cmt_skipped;
    struct cmt_counter *cmt_refresh;
    struct cmt_counter *cmt_refresh_errors;
#endif
};

extern struct flb_filter_plugin filter_lookup_plugin;

#endif