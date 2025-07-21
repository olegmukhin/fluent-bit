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

#include <fluent-bit/flb_filter_plugin.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_hash_table.h>
#include <fluent-bit/flb_config_map.h>
#include <fluent-bit/flb_log_event_decoder.h>
#include <fluent-bit/flb_log_event_encoder.h>
#include <fluent-bit/flb_ra_key.h>
#include <fluent-bit/flb_record_accessor.h>
#include <fluent-bit/flb_metrics.h>
#include <fluent-bit/flb_scheduler.h>
#include <cfl/cfl_time.h>
#include <monkey/mk_core/mk_list.h>
#include <msgpack.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>

#include "lookup.h"

/* Improved metric macros using cmt_counter_inc for consistency with other filters */
#ifdef FLB_HAVE_METRICS
#define INCREMENT_SKIPPED_METRIC(ctx, ins) do { \
    uint64_t ts = cfl_time_now(); \
    if (ctx->cmt_skipped) { \
        cmt_counter_inc(ctx->cmt_skipped, ts, 1, (char *[]) {(char*)flb_filter_name(ins)}); \
    } \
    flb_metrics_sum(FLB_LOOKUP_METRIC_SKIPPED, 1, ins->metrics); \
} while(0)

#define INCREMENT_MATCHED_METRIC(ctx, ins) do { \
    uint64_t ts = cfl_time_now(); \
    if (ctx->cmt_matched) { \
        cmt_counter_inc(ctx->cmt_matched, ts, 1, (char *[]) {(char*)flb_filter_name(ins)}); \
    } \
    flb_metrics_sum(FLB_LOOKUP_METRIC_MATCHED, 1, ins->metrics); \
} while(0)

#define INCREMENT_PROCESSED_METRIC(ctx, ins) do { \
    uint64_t ts = cfl_time_now(); \
    if (ctx->cmt_processed) { \
        cmt_counter_inc(ctx->cmt_processed, ts, 1, (char *[]) {(char*)flb_filter_name(ins)}); \
    } \
    flb_metrics_sum(FLB_LOOKUP_METRIC_PROCESSED, 1, ins->metrics); \
} while(0)

#define INCREMENT_REFRESH_METRIC(ctx, ins) do { \
    uint64_t ts = cfl_time_now(); \
    if (ctx->cmt_refresh) { \
        cmt_counter_inc(ctx->cmt_refresh, ts, 1, (char *[]) {(char*)flb_filter_name(ins)}); \
    } \
    flb_metrics_sum(FLB_LOOKUP_METRIC_REFRESH, 1, ins->metrics); \
} while(0)

#define INCREMENT_REFRESH_ERRORS_METRIC(ctx, ins) do { \
    uint64_t ts = cfl_time_now(); \
    if (ctx->cmt_refresh_errors) { \
        cmt_counter_inc(ctx->cmt_refresh_errors, ts, 1, (char *[]) {(char*)flb_filter_name(ins)}); \
    } \
    flb_metrics_sum(FLB_LOOKUP_METRIC_REFRESH_ERRORS, 1, ins->metrics); \
} while(0)
#else
#define INCREMENT_SKIPPED_METRIC(ctx, ins) do { } while(0)
#define INCREMENT_MATCHED_METRIC(ctx, ins) do { } while(0)
#define INCREMENT_PROCESSED_METRIC(ctx, ins) do { } while(0)
#define INCREMENT_REFRESH_METRIC(ctx, ins) do { } while(0)
#define INCREMENT_REFRESH_ERRORS_METRIC(ctx, ins) do { } while(0)
#endif


struct val_node {
    struct mk_list _head;
    void *val;
};

/* Forward declarations */
static int load_csv(struct lookup_ctx *ctx);
static void refresh_timer_callback(struct flb_config *config, void *data);
static size_t count_csv_lines(const char *filename);
static size_t calculate_hash_table_size(size_t expected_entries);

/*
 * Trims leading/trailing whitespace and optionally normalizes to lower-case.
 * Allocates output buffer (caller must free if output != input).
 */
static int normalize_and_trim(const char *input, size_t len, int ignore_case, char **output, size_t *out_len)
{
    if (!input || len == 0) {
        *output = NULL;
        *out_len = 0;
        return 0;
    }
    /* Trim leading whitespace */
    const char *start = input;
    size_t n = len;
    while (n > 0 && isspace((unsigned char)*start)) {
        start++;
        n--;
    }
    /* Trim trailing whitespace */
    const char *end = start + n;
    while (n > 0 && isspace((unsigned char)*(end - 1))) {
        end--;
        n--;
    }
    if (n == 0) {
        *output = NULL;
        *out_len = 0;
        return 0;
    }
    if (ignore_case) {
        char *buf = flb_malloc(n + 1);
        if (!buf) {
            *output = NULL;
            *out_len = 0;
            return -1;
        }
        for (size_t j = 0; j < n; j++) {
            buf[j] = tolower((unsigned char)start[j]);
        }
        buf[n] = '\0';
        *output = buf;
        *out_len = n;
        return 1;
    } else {
        *output = (char *)start;
        *out_len = n;
        return 0;
    }
}

/* Dynamic buffer structure for growing strings */
struct dynamic_buffer {
    char *data;
    size_t len;
    size_t capacity;
};

/* Initialize a dynamic buffer */
static int dynbuf_init(struct dynamic_buffer *buf, size_t initial_capacity)
{
    if (initial_capacity == 0) {
        return -1;  /* Fail with zero capacity */
    }
    buf->data = flb_malloc(initial_capacity);
    if (!buf->data) {
        return -1;
    }
    buf->len = 0;
    buf->capacity = initial_capacity;
    buf->data[0] = '\0';
    return 0;
}

/* Append a character to dynamic buffer, growing if necessary */
static int dynbuf_append_char(struct dynamic_buffer *buf, char c)
{
    /* Ensure we have space for the character plus null terminator */
    if (buf->len + 1 >= buf->capacity) {
        size_t new_capacity = buf->capacity * 2;
        char *new_data = flb_realloc(buf->data, new_capacity);
        if (!new_data) {
            return -1;
        }
        buf->data = new_data;
        buf->capacity = new_capacity;
    }
    buf->data[buf->len++] = c;
    buf->data[buf->len] = '\0';
    return 0;
}

/* Free dynamic buffer */
static void dynbuf_destroy(struct dynamic_buffer *buf)
{
    if (buf && buf->data) {
        flb_free(buf->data);
        buf->data = NULL;
        buf->len = 0;
        buf->capacity = 0;
    }
}

/* Read a line of arbitrary length from file using dynamic allocation */
static char *read_line_dynamic(FILE *fp, size_t *line_length)
{
    size_t capacity = 256;  /* Initial capacity */
    size_t len = 0;
    char *line = flb_malloc(capacity);
    int c;
    
    if (!line) {
        return NULL;
    }
    
    while ((c = fgetc(fp)) != EOF) {
        /* Check if we need to grow the buffer */
        if (len + 1 >= capacity) {
            size_t new_capacity = capacity * 2;
            char *new_line = flb_realloc(line, new_capacity);
            if (!new_line) {
                flb_free(line);
                return NULL;
            }
            line = new_line;
            capacity = new_capacity;
        }
        
        /* Add character to buffer */
        line[len++] = c;
        
        /* Check for end of line */
        if (c == '\n') {
            break;
        }
    }
    
    /* If we read nothing and hit EOF, return NULL */
    if (len == 0 && c == EOF) {
        flb_free(line);
        return NULL;
    }
    
    /* Null terminate the string */
    if (len >= capacity) {
        char *new_line = flb_realloc(line, len + 1);
        if (!new_line) {
            flb_free(line);
            return NULL;
        }
        line = new_line;
    }
    line[len] = '\0';
    
    /* Remove trailing \r\n characters */
    while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
        line[--len] = '\0';
    }
    
    if (line_length) {
        *line_length = len;
    }
    
    return line;
}

/* Simplified refresh management functions with timer optimization */
static void handle_refresh_result(struct lookup_ctx *ctx, int success)
{
    time_t now = time(NULL);
    
    if (success) {
        if (ctx->circuit_state == REFRESH_RETRY) {
            flb_plg_info(ctx->ins, "CSV refresh recovered after %d failures, resuming normal refresh interval (%d seconds)",
                         ctx->refresh_failures, ctx->refresh_interval);
        }
        ctx->circuit_state = REFRESH_NORMAL;
        ctx->refresh_failures = 0;
        ctx->last_successful_refresh = now;
        
        /* Reschedule timer to use normal refresh_interval (only if different from current) */
        if (ctx->refresh_timer && ctx->refresh_interval > 0) {
            flb_sched_timer_destroy(ctx->refresh_timer);
            ctx->refresh_timer = NULL;
            
            int ret = flb_sched_timer_cb_create(ctx->ins->config->sched,
                                              FLB_SCHED_TIMER_CB_PERM,
                                              ctx->refresh_interval * 1000,
                                              refresh_timer_callback,
                                              ctx,
                                              &ctx->refresh_timer);
            if (ret != 0) {
                flb_plg_warn(ctx->ins, "Failed to reschedule normal refresh timer");
            }
        }
    } else {
        ctx->refresh_failures++;
        
        /* Increment refresh errors counter */
        INCREMENT_REFRESH_ERRORS_METRIC(ctx, ctx->ins);
        
        if (ctx->circuit_state == REFRESH_NORMAL) {
            flb_plg_warn(ctx->ins, "CSV refresh failed, switching to retry mode (retry every %d seconds)", 
                         ctx->retry_interval);
            ctx->circuit_state = REFRESH_RETRY;
            
            /* Reschedule timer to use faster retry_interval (CPU optimized) */
            if (ctx->refresh_timer) {
                flb_sched_timer_destroy(ctx->refresh_timer);
                ctx->refresh_timer = NULL;
                
                int ret = flb_sched_timer_cb_create(ctx->ins->config->sched,
                                                  FLB_SCHED_TIMER_CB_PERM,
                                                  ctx->retry_interval * 1000,
                                                  refresh_timer_callback,
                                                  ctx,
                                                  &ctx->refresh_timer);
                if (ret != 0) {
                    flb_plg_warn(ctx->ins, "Failed to reschedule retry timer");
                }
            }
        } else {
            /* Already in retry mode, continue retrying at current interval */
            flb_plg_debug(ctx->ins, "CSV refresh retry failed (attempt %d), next retry in %d seconds", 
                          ctx->refresh_failures, ctx->retry_interval);
        }
    }
    
    ctx->last_refresh = now;
}

/* Helper function to safely cleanup val_list */
static void cleanup_val_list(struct mk_list *val_list)
{
    struct mk_list *tmp;
    struct mk_list *head;
    struct val_node *node;
    
    mk_list_foreach_safe(head, tmp, val_list) {
        node = mk_list_entry(head, struct val_node, _head);
        if (node->val) {
            flb_free(node->val);
        }
        mk_list_del(head);
        flb_free(node);
    }
}

/* Thread-safe CSV refresh with graceful degradation - SIMPLIFIED */
static int refresh_csv_safe(struct lookup_ctx *ctx)
{
    struct flb_hash_table *new_ht = NULL;
    struct mk_list new_val_list;
    int load_result = -1;
    
    flb_plg_debug(ctx->ins, "Starting CSV refresh attempt %d", ctx->refresh_failures + 1);
    
    /* Increment refresh attempts counter */
    INCREMENT_REFRESH_METRIC(ctx, ctx->ins);
    
    /* Count CSV lines for optimal hash table sizing */
    size_t estimated_entries = count_csv_lines(ctx->file);
    size_t hash_table_size = calculate_hash_table_size(estimated_entries);
    
    flb_plg_debug(ctx->ins, "CSV refresh: estimated %zu entries, using hash table size %zu (load factor ~%.2f)", 
                  estimated_entries, hash_table_size, 
                  estimated_entries > 0 ? (double)estimated_entries / hash_table_size : 0.0);
    
    /* Create new hash table for loading with optimal size */
    new_ht = flb_hash_table_create(FLB_HASH_TABLE_EVICT_NONE, hash_table_size, -1);
    if (!new_ht) {
        flb_plg_error(ctx->ins, "CSV refresh failed: cannot create new hash table (size %zu)", hash_table_size);
        INCREMENT_REFRESH_ERRORS_METRIC(ctx, ctx->ins);
        handle_refresh_result(ctx, 0);
        return -1;
    }
    
    mk_list_init(&new_val_list);
    
    /* Check file accessibility before attempting load (CPU optimization) */
    if (access(ctx->file, R_OK) != 0) {
        flb_plg_warn(ctx->ins, "CSV refresh failed: file '%s' not accessible: %s", 
                     ctx->file, strerror(errno));
        flb_hash_table_destroy(new_ht);
        INCREMENT_REFRESH_ERRORS_METRIC(ctx, ctx->ins);
        handle_refresh_result(ctx, 0);
        return -1;
    }
    
    /* Temporarily switch context to load into new structures - acquire write lock for exclusive access */
    pthread_rwlock_wrlock(&ctx->refresh_rwlock);
    
    struct flb_hash_table *old_ht = ctx->ht;
    struct mk_list old_val_list = ctx->val_list;
    
    ctx->ht = new_ht;
    ctx->val_list = new_val_list;
    
    /* Attempt to load CSV into new structures */
    load_result = load_csv(ctx);
    
    if (load_result == 0) {
        /* Success: Replace old structures with new ones */
        double new_load_factor = (double)ctx->ht->total_count / hash_table_size;
        double old_load_factor = old_ht->size > 0 ? (double)old_ht->total_count / old_ht->size : 0.0;
        
        flb_plg_info(ctx->ins, "CSV refresh successful: loaded %d entries (was %d), load factor %.3f (was %.3f)", 
                     (int)ctx->ht->total_count, (int)old_ht->total_count, new_load_factor, old_load_factor);
        
        /* Clean up old structures */
        flb_hash_table_destroy(old_ht);
        cleanup_val_list(&old_val_list);
        
        /* Update success metrics */
        handle_refresh_result(ctx, 1);
        
    } else {
        /* Failure: Restore old structures, cleanup new ones */
        flb_plg_warn(ctx->ins, "CSV refresh failed: keeping existing %d entries", 
                     (int)old_ht->total_count);
        
        /* Restore old structures */
        flb_hash_table_destroy(ctx->ht);
        cleanup_val_list(&ctx->val_list);
        
        ctx->ht = old_ht;
        ctx->val_list = old_val_list;
        
        /* Handle failure - switches to retry mode with faster interval */
        handle_refresh_result(ctx, 0);
    }
    
    pthread_rwlock_unlock(&ctx->refresh_rwlock);
    
    return load_result;
}

/* Timer callback for periodic refresh - CPU optimized */
static void refresh_timer_callback(struct flb_config *config, void *data)
{
    struct lookup_ctx *ctx = (struct lookup_ctx *)data;
    
    if (!ctx || ctx->refresh_interval <= 0) {
        return;
    }
    
    /* CPU optimization: Quick pre-check for file availability in retry mode */
    if (ctx->circuit_state == REFRESH_RETRY) {
        if (access(ctx->file, R_OK) != 0) {
            /* File still not accessible, avoid expensive operations */
            ctx->refresh_failures++;
            ctx->last_refresh = time(NULL);
            flb_plg_debug(ctx->ins, "Quick retry check: file still unavailable (failure %d)", 
                          ctx->refresh_failures);
            return;
        }
    }
    
    flb_plg_debug(ctx->ins, "Timer triggered CSV refresh");
    refresh_csv_safe(ctx);
}

/* 
 * Count lines in CSV file for optimal hash table sizing.
 * Uses buffered reading for performance with large files.
 */
static size_t count_csv_lines(const char *filename)
{
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        return 0;  /* Will fall back to default sizing */
    }
    
    size_t line_count = 0;
    size_t buffer_size = 65536;  /* 64KB buffer for efficient reading */
    char *buffer = flb_malloc(buffer_size);
    
    if (!buffer) {
        fclose(fp);
        return 0;  /* Fall back to default sizing on memory failure */
    }
    
    size_t bytes_read;
    while ((bytes_read = fread(buffer, 1, buffer_size, fp)) > 0) {
        for (size_t i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                line_count++;
            }
        }
    }
    
    flb_free(buffer);
    fclose(fp);
    
    /* Subtract 1 for header line if we found any lines */
    return line_count > 0 ? line_count - 1 : 0;
}

/*
 * Calculate optimal hash table size based on expected entry count.
 * Target load factor: ~0.67 (1.5x entries) for optimal performance.
 */
static size_t calculate_hash_table_size(size_t expected_entries)
{
    size_t min_size = 64;    /* Minimum reasonable size */
    size_t max_size = 1048576; /* Maximum size (1M buckets) */
    
    if (expected_entries == 0) {
        return 1024;  /* Default size for unknown count */
    }
    
    /* Target load factor: 0.67, so size = entries * 1.5 */
    size_t target_size = expected_entries + (expected_entries / 2);
    
    /* Round up to next power of 2 for better hash distribution */
    size_t size = min_size;
    while (size < target_size && size < max_size) {
        size *= 2;
    }
    
    /* Clamp to reasonable bounds */
    if (size < min_size) size = min_size;
    if (size > max_size) size = max_size;
    
    return size;
}

static int load_csv(struct lookup_ctx *ctx)
{
    FILE *fp;
    int line_num = 1;
    fp = fopen(ctx->file, "r");
    if (!fp) {
        flb_plg_error(ctx->ins, "cannot open CSV file '%s': %s", ctx->file, strerror(errno));
        return -1;
    }
    /* Initialize value list if not already */
    mk_list_init(&ctx->val_list);
    
    /* Skip header using dynamic line reading */
    char *header_line = read_line_dynamic(fp, NULL);
    if (!header_line) {
        flb_plg_error(ctx->ins, "empty CSV file: %s", ctx->file);
        fclose(fp);
        return -1;
    }
    flb_free(header_line);  /* Free the header line as we don't need it */
    
    char *line;
    size_t line_length;
    while ((line = read_line_dynamic(fp, &line_length)) != NULL) {
        if (line_length == 0) {
            flb_free(line);
            line_num++;
            continue;
        }

        /* Handle quotes in CSV files using dynamic buffers */
        char *p = line;
        struct dynamic_buffer key_buf, val_buf;
        int in_quotes = 0;
        int field = 0; /* 0=key, 1=val */

        /* Initialize dynamic buffers */
        if (dynbuf_init(&key_buf, 256) != 0) {
            flb_plg_debug(ctx->ins, "Failed to initialize key buffer for line %d", line_num);
            flb_free(line);
            line_num++;
            continue;
        }
        if (dynbuf_init(&val_buf, 256) != 0) {
            flb_plg_debug(ctx->ins, "Failed to initialize value buffer for line %d", line_num);
            dynbuf_destroy(&key_buf);
            flb_free(line);
            line_num++;
            continue;
        }

        /* Parse key from first column (and handle quotes) */
        while (*p && (field == 0)) {
            if (!in_quotes && *p == '"') {
                in_quotes = 1;
                p++;
                continue;
            }
            if (in_quotes) {
                if (*p == '"') {
                    if (*(p+1) == '"') {
                        /* Escaped quote */
                        if (dynbuf_append_char(&key_buf, '"') != 0) {
                            flb_plg_debug(ctx->ins, "Buffer allocation failed for line %d", line_num);
                            dynbuf_destroy(&key_buf);
                            dynbuf_destroy(&val_buf);
                            flb_free(line);
                            line_num++;
                            goto next_line;
                        }
                        p += 2;
                        continue;
                    } else {
                        in_quotes = 0;
                        p++;
                        continue;
                    }
                }
                if (dynbuf_append_char(&key_buf, *p) != 0) {
                    flb_plg_debug(ctx->ins, "Buffer allocation failed for line %d", line_num);
                    dynbuf_destroy(&key_buf);
                    dynbuf_destroy(&val_buf);
                    flb_free(line);
                    line_num++;
                    goto next_line;
                }
                p++;
                continue;
            }
            if (*p == ctx->delimiter[0]) {
                field = 1;
                p++;
                break;
            }
            if (dynbuf_append_char(&key_buf, *p) != 0) {
                flb_plg_debug(ctx->ins, "Buffer allocation failed for line %d", line_num);
                dynbuf_destroy(&key_buf);
                dynbuf_destroy(&val_buf);
                flb_free(line);
                line_num++;
                goto next_line;
            }
            p++;
        }

        /* Parse value from second column (handle quotes) */
        in_quotes = 0;
        while (*p && (field == 1)) {
            if (!in_quotes && *p == '"') {
                in_quotes = 1;
                p++;
                continue;
            }
            if (in_quotes) {
                if (*p == '"') {
                    if (*(p+1) == '"') {
                        // Escaped quote
                        if (dynbuf_append_char(&val_buf, '"') != 0) {
                            flb_plg_error(ctx->ins, "Failed to append to value buffer for line %d", line_num);
                            dynbuf_destroy(&key_buf);
                            dynbuf_destroy(&val_buf);
                            flb_free(line);
                            line_num++;
                            goto next_line;
                        }
                        p += 2;
                        continue;
                    } else {
                        in_quotes = 0;
                        p++;
                        continue;
                    }
                }
                if (dynbuf_append_char(&val_buf, *p) != 0) {
                    flb_plg_error(ctx->ins, "Failed to append to value buffer for line %d", line_num);
                    dynbuf_destroy(&key_buf);
                    dynbuf_destroy(&val_buf);
                    flb_free(line);
                    line_num++;
                    goto next_line;
                }
                p++;
                continue;
            }
            if (*p == ctx->delimiter[0]) {
                /* Ignore extra fields */
                break;
            }
            if (dynbuf_append_char(&val_buf, *p) != 0) {
                flb_plg_error(ctx->ins, "Failed to append to value buffer for line %d", line_num);
                dynbuf_destroy(&key_buf);
                dynbuf_destroy(&val_buf);
                flb_free(line);
                line_num++;
                goto next_line;
            }
            p++;
        }

        /* Check for unmatched quote: if in_quotes is set, log warning and skip line */
        if (in_quotes) {
            flb_plg_warn(ctx->ins, "Unmatched quote in line %d, skipping", line_num);
            dynbuf_destroy(&key_buf);
            dynbuf_destroy(&val_buf);
            flb_free(line);
            line_num++;
            continue;
        }

        /* Normalize and trim key */
        char *key_ptr = NULL;
        size_t key_len = 0;
        int key_ptr_allocated = normalize_and_trim(key_buf.data, key_buf.len, ctx->ignore_case, &key_ptr, &key_len);
        if (key_ptr_allocated < 0) {
            dynbuf_destroy(&key_buf);
            dynbuf_destroy(&val_buf);
            flb_free(line);
            line_num++;
            continue;
        }
        /* Normalize and trim value */
        char *val_ptr = NULL;
        size_t val_len = 0;
        int val_ptr_allocated = normalize_and_trim(val_buf.data, val_buf.len, 0, &val_ptr, &val_len);
        if (val_ptr_allocated < 0) {
            if (key_ptr_allocated) flb_free(key_ptr);
            dynbuf_destroy(&key_buf);
            dynbuf_destroy(&val_buf);
            flb_free(line);
            line_num++;
            continue;
        }
        if (key_len == 0) {
            /* Empty key is not valid, skip */
            if (key_ptr_allocated) flb_free(key_ptr);
            if (val_ptr_allocated) flb_free(val_ptr);
            dynbuf_destroy(&key_buf);
            dynbuf_destroy(&val_buf);
            flb_free(line);
            line_num++;
            continue;
        }
        /* Explicitly duplicate value buffer for hash table safety, allocate +1 for null terminator */
        char *val_heap = flb_malloc(val_len + 1);
        if (!val_heap) {
            if (key_ptr_allocated) flb_free(key_ptr);
            if (val_ptr_allocated) flb_free(val_ptr);
            dynbuf_destroy(&key_buf);
            dynbuf_destroy(&val_buf);
            flb_free(line);
            line_num++;
            continue;
        }
        if (val_len > 0 && val_ptr) {
            memcpy(val_heap, val_ptr, val_len);
        } else {
            /* For empty values, ensure val_heap is initialized */
            val_len = 0;
        }
        val_heap[val_len] = '\0';
        int ret = flb_hash_table_add(ctx->ht, key_ptr, key_len, val_heap, val_len);
        if (ret < 0) {
            flb_free(val_heap);
            flb_plg_warn(ctx->ins, "Failed to add key '%.*s' (duplicate or error), skipping", (int)key_len, key_ptr);
            if (key_ptr_allocated) flb_free(key_ptr);
            if (val_ptr_allocated) flb_free(val_ptr);
            dynbuf_destroy(&key_buf);
            dynbuf_destroy(&val_buf);
            flb_free(line);
            line_num++;
            continue;
        }
        /* Track allocated value for later cleanup */
        struct val_node *node = flb_malloc(sizeof(struct val_node));
        if (node) {
            node->val = val_heap;
            mk_list_add(&node->_head, &ctx->val_list);
        } else {
            /* If malloc fails, value will leak, but plugin will still function */
            flb_plg_warn(ctx->ins, "Failed to allocate val_node for value cleanup, value will leak");
        }
        /* Do not free val_heap; hash table owns it now */
        if (key_ptr_allocated) flb_free(key_ptr);
        if (val_ptr_allocated) flb_free(val_ptr);
        dynbuf_destroy(&key_buf);
        dynbuf_destroy(&val_buf);
        flb_free(line);
        line_num++;
        continue;

        next_line:
        /* Label for error handling - cleanup already done in error paths */
        continue;
    }
    fclose(fp);
    return 0;
}

static int cb_lookup_init(struct flb_filter_instance *ins,
                         struct flb_config *config,
                         void *data)
{
    int ret;
    /*
     * Allocate and initialize the filter context for this plugin instance.
     * This context will hold configuration, hash table, and state.
     */
    struct lookup_ctx *ctx;
    ctx = flb_calloc(1, sizeof(struct lookup_ctx));
    if (!ctx) {
        flb_errno();
        return -1;
    }
    ctx->ins = ins;

    /* Initialize refresh system defaults */
    ctx->circuit_state = REFRESH_NORMAL;
    ctx->refresh_failures = 0;
    ctx->last_refresh = 0;
    ctx->last_successful_refresh = time(NULL);
    ctx->refresh_timer = NULL;
    
    /* Initialize read-write lock for thread-safe refresh - allows concurrent filter operations */
    if (pthread_rwlock_init(&ctx->refresh_rwlock, NULL) != 0) {
        flb_plg_error(ins, "Failed to initialize refresh rwlock");
        flb_free(ctx);
        return -1;
    }

#ifdef FLB_HAVE_METRICS
    /* Initialize CMT metrics with error checking */
    ctx->cmt_processed = cmt_counter_create(ins->cmt,
                                            "fluentbit", "filter", "lookup_processed_records_total",
                                            "Total number of records processed by lookup filter",
                                            1, (char *[]) {"name"});
    if (!ctx->cmt_processed) {
        flb_plg_error(ins, "could not create processed records metric");
        goto error;
    }

    ctx->cmt_matched = cmt_counter_create(ins->cmt,
                                          "fluentbit", "filter", "lookup_matched_records_total",
                                          "Total number of records matched in lookup table",
                                          1, (char *[]) {"name"});
    if (!ctx->cmt_matched) {
        flb_plg_error(ins, "could not create matched records metric");
        goto error;
    }

    ctx->cmt_skipped = cmt_counter_create(ins->cmt,
                                          "fluentbit", "filter", "lookup_skipped_records_total",
                                          "Total number of records skipped due to errors or missing keys",
                                          1, (char *[]) {"name"});
    if (!ctx->cmt_skipped) {
        flb_plg_error(ins, "could not create skipped records metric");
        goto error;
    }

    ctx->cmt_refresh = cmt_counter_create(ins->cmt,
                                          "fluentbit", "filter", "lookup_refresh_total",
                                          "Total number of lookup table refresh attempts",
                                          1, (char *[]) {"name"});
    if (!ctx->cmt_refresh) {
        flb_plg_error(ins, "could not create refresh metric");
        goto error;
    }

    ctx->cmt_refresh_errors = cmt_counter_create(ins->cmt,
                                                 "fluentbit", "filter", "lookup_refresh_errors_total",
                                                 "Total number of lookup table refresh errors",
                                                 1, (char *[]) {"name"});
    if (!ctx->cmt_refresh_errors) {
        flb_plg_error(ins, "could not create refresh errors metric");
        goto error;
    }

    /* Initialize legacy metrics */
    flb_metrics_add(FLB_LOOKUP_METRIC_PROCESSED, "lookup_processed_records_total", ins->metrics);
    flb_metrics_add(FLB_LOOKUP_METRIC_MATCHED, "lookup_matched_records_total", ins->metrics);
    flb_metrics_add(FLB_LOOKUP_METRIC_SKIPPED, "lookup_skipped_records_total", ins->metrics);
    flb_metrics_add(FLB_LOOKUP_METRIC_REFRESH, "lookup_refresh_total", ins->metrics);
    flb_metrics_add(FLB_LOOKUP_METRIC_REFRESH_ERRORS, "lookup_refresh_errors_total", ins->metrics);
#endif

    /*
     * Populate context fields from config_map. This sets file, lookup_key,
     * result_key, and ignore_case from the configuration.
     */
    ret = flb_filter_config_map_set(ins, ctx);
    if (ret == -1) {
        flb_free(ctx);
        return -1;
    }

    /*
     * Validate required configuration options. All three must be set for
     * the filter to operate.
     */
    if (!ctx->file || !ctx->lookup_key || !ctx->result_key) {
        flb_plg_error(ins, "missing required config: file, lookup_key, result_key");
        goto error;
    }

    /* Validate delimiter - must be a single character */
    if (!ctx->delimiter || strlen(ctx->delimiter) != 1) {
        flb_plg_error(ins, "delimiter must be a single character, got: %s", 
                      ctx->delimiter ? ctx->delimiter : "NULL");
        goto error;
    }

    /* Validate refresh configuration - SIMPLIFIED */
    if (ctx->refresh_interval < 0) {
        flb_plg_error(ins, "refresh_interval must be >= 0, got: %d", ctx->refresh_interval);
        goto error;
    }
    
    if (ctx->retry_interval < 1) {
        flb_plg_error(ins, "retry_interval must be >= 1, got: %d", ctx->retry_interval);
        goto error;
    }
    
    /* Warn if retry_interval is longer than refresh_interval (could be inefficient) */
    if (ctx->refresh_interval > 0 && ctx->retry_interval > ctx->refresh_interval) {
        flb_plg_warn(ins, "retry_interval (%d) is longer than refresh_interval (%d), consider adjusting", 
                     ctx->retry_interval, ctx->refresh_interval);
    }

    /* Check file existence and readability */
    if (access(ctx->file, R_OK) != 0) {
        flb_plg_error(ins, "CSV file '%s' does not exist or is not readable: %s", ctx->file, strerror(errno));
        goto error;
    }

    /*
     * Count CSV entries and create optimally-sized hash table for lookups.
     * This prevents long collision chains that hurt performance with large files.
     */
    flb_plg_info(ins, "Starting dynamic hash table sizing for file: %s", ctx->file);
    size_t estimated_entries = count_csv_lines(ctx->file);
    size_t hash_table_size = calculate_hash_table_size(estimated_entries);
    
    flb_plg_info(ins, "CSV analysis: estimated %zu entries, creating hash table with %zu buckets (target load factor ~0.67)", 
                 estimated_entries, hash_table_size);
    
    ctx->ht = flb_hash_table_create(FLB_HASH_TABLE_EVICT_NONE, hash_table_size, -1);
    if (!ctx->ht) {
        flb_plg_error(ins, "could not create hash table (size %zu)", hash_table_size);
        goto error;
    }

    /* Initialize record accessor for lookup_key */
    ctx->ra_lookup_key = flb_ra_create(ctx->lookup_key, FLB_TRUE);
    if (!ctx->ra_lookup_key) {
        flb_plg_error(ins, "invalid lookup_key pattern: %s", ctx->lookup_key);
        goto error;
    }

    /* Load CSV data into hash table. */
    ret = load_csv(ctx);
    if (ret < 0) {
        goto error;
    }
    
    /* Calculate actual load factor achieved */
    double actual_load_factor = (double)ctx->ht->total_count / hash_table_size;
    
    flb_plg_info(ins, "Loaded %d entries from CSV file '%s' into %zu-bucket hash table (load factor: %.3f)", 
                 (int)ctx->ht->total_count, ctx->file, hash_table_size, actual_load_factor);
    flb_plg_info(ins, "Lookup filter initialized: lookup_key='%s', result_key='%s', delimiter='%s', ignore_case=%s", 
                 ctx->lookup_key, ctx->result_key, ctx->delimiter, ctx->ignore_case ? "true" : "false");

    /* Setup periodic refresh timer if enabled */
    if (ctx->refresh_interval > 0) {
        ret = flb_sched_timer_cb_create(config->sched,
                                        FLB_SCHED_TIMER_CB_PERM,
                                        ctx->refresh_interval * 1000, /* Convert to milliseconds */
                                        refresh_timer_callback,
                                        ctx,
                                        &ctx->refresh_timer);
        if (ret != 0) {
            flb_plg_warn(ins, "Failed to create refresh timer, refresh disabled");
            ctx->refresh_interval = 0;  /* Disable refresh */
        } else {
            flb_plg_info(ins, "CSV refresh enabled: interval=%d seconds, retry_interval=%d seconds",
                         ctx->refresh_interval, ctx->retry_interval);
        }
    }

    /* Store context for use in filter and exit callbacks. */
    flb_filter_set_context(ins, ctx);
    return 0;

error:
    if (ctx->refresh_timer) {
        flb_sched_timer_destroy(ctx->refresh_timer);
    }
    if (ctx->ra_lookup_key) {
        flb_ra_destroy(ctx->ra_lookup_key);
    }
    if (ctx->ht) {
        flb_hash_table_destroy(ctx->ht);
    }
    pthread_rwlock_destroy(&ctx->refresh_rwlock);
    flb_free(ctx);
    return -1;
}

static int emit_original_record(
    struct flb_log_event_encoder *log_encoder,
    struct flb_log_event *log_event,
    struct flb_filter_instance *ins,
    struct lookup_ctx *ctx,
    int rec_num)
{
    int ret = flb_log_event_encoder_begin_record(log_encoder);
    if (ret == FLB_EVENT_ENCODER_SUCCESS) {
        ret = flb_log_event_encoder_set_timestamp(log_encoder, &log_event->timestamp);
    }
    if (ret == FLB_EVENT_ENCODER_SUCCESS && log_event->metadata) {
        ret = flb_log_event_encoder_set_metadata_from_msgpack_object(log_encoder, log_event->metadata);
    }
    if (ret == FLB_EVENT_ENCODER_SUCCESS) {
        ret = flb_log_event_encoder_set_body_from_msgpack_object(log_encoder, log_event->body);
    }
    if (ret == FLB_EVENT_ENCODER_SUCCESS) {
        ret = flb_log_event_encoder_commit_record(log_encoder);
    } else {
        flb_log_event_encoder_rollback_record(log_encoder);
        flb_plg_warn(ins, "Record %d: failed to encode original record, skipping", rec_num);
        if (ctx) {
            INCREMENT_SKIPPED_METRIC(ctx, ins);
        }
    }
    return ret;
}

static int cb_lookup_filter(const void *data, size_t bytes,
                           const char *tag, int tag_len,
                           void **out_buf, size_t *out_bytes,
                           struct flb_filter_instance *ins,
                           struct flb_input_instance *in_ins,
                           void *context,
                           struct flb_config *config)
{
    /*
     * Main filter callback: processes each log event in the input batch.
     * For each record, attempts to look up a value in the hash table using
     * the configured key. If found, adds result_key to the record; otherwise,
     * emits the original record unchanged.
     */
    struct lookup_ctx *ctx = context;
    struct flb_log_event_decoder log_decoder;
    struct flb_log_event_encoder log_encoder;
    struct flb_log_event log_event;
    int ret;
    int rec_num = 0;
    void *found_val = NULL;
    size_t found_len = 0;
    char *lookup_val_str = NULL;
    size_t lookup_val_len = 0;
    int lookup_val_allocated = 0;
    bool any_modified = false;  /* Track if any records were modified */

    /* Ensure context is valid */
    if (!ctx) {
        flb_plg_error(ins, "lookup filter context is NULL");
        return FLB_FILTER_NOTOUCH;
    }

    /* Acquire read lock for the duration of filtering to allow concurrent filter operations */
    pthread_rwlock_rdlock(&ctx->refresh_rwlock);

    /* Initialize log event decoder for input records */
    ret = flb_log_event_decoder_init(&log_decoder, (char *)data, bytes);
    if (ret != FLB_EVENT_DECODER_SUCCESS) {
        flb_plg_error(ins, "Log event decoder initialization error : %d", ret);
        pthread_rwlock_unlock(&ctx->refresh_rwlock);
        return FLB_FILTER_NOTOUCH;
    }

    /* Initialize log event encoder for output records */
    ret = flb_log_event_encoder_init(&log_encoder, FLB_LOG_EVENT_FORMAT_DEFAULT);
    if (ret != FLB_EVENT_ENCODER_SUCCESS) {
        flb_plg_error(ins, "Log event encoder initialization error : %d", ret);
        flb_log_event_decoder_destroy(&log_decoder);
        pthread_rwlock_unlock(&ctx->refresh_rwlock);
        return FLB_FILTER_NOTOUCH;
    }

    /* Process each log event in the input batch */
    while ((ret = flb_log_event_decoder_next(&log_decoder, &log_event)) == FLB_EVENT_DECODER_SUCCESS) {
        rec_num++;
        /* Count all records processed by the filter */
        INCREMENT_PROCESSED_METRIC(ctx, ins);
        lookup_val_str = NULL;
        lookup_val_len = 0;
        lookup_val_allocated = 0;
        char *dynamic_val_buf = NULL; /* Track dynamic buffer for numeric conversions */

        /* Helper macro to clean up dynamic buffer and allocated lookup strings */
        #define CLEANUP_DYNAMIC_BUFFERS() do { \
            if (dynamic_val_buf) { \
                flb_free(dynamic_val_buf); \
                dynamic_val_buf = NULL; \
            } \
            if (lookup_val_allocated && lookup_val_str) { \
                flb_free(lookup_val_str); \
                lookup_val_str = NULL; \
            } \
        } while(0)

        /* If body is not a map, skip matching logic and emit original record */
        if (!log_event.body || log_event.body->type != MSGPACK_OBJECT_MAP) {
            flb_plg_debug(ins, "Record %d: body is not a map (type=%d), skipping lookup", rec_num, log_event.body ? log_event.body->type : -1);
            INCREMENT_SKIPPED_METRIC(ctx, ins);
            emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
            continue;
        }

        /* Use record accessor to get the lookup value */
        struct flb_ra_value *rval = flb_ra_get_value_object(ctx->ra_lookup_key, *log_event.body);
        if (!rval) {
            /* Key not found, skip matching logic and emit original record */
            flb_plg_debug(ins, "Record %d: lookup_key '%s' not found, skipping lookup", rec_num, ctx->lookup_key);
            INCREMENT_SKIPPED_METRIC(ctx, ins);
            emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
            continue;
        }

        /* Extract string value from record accessor result */
        if (rval->type == FLB_RA_STRING) {
            lookup_val_allocated = normalize_and_trim((char *)rval->o.via.str.ptr, rval->o.via.str.size, ctx->ignore_case, &lookup_val_str, &lookup_val_len);
            if (lookup_val_allocated < 0) {
                flb_plg_warn(ins, "Record %d: malloc failed for normalize_and_trim (string), skipping", rec_num);
                INCREMENT_SKIPPED_METRIC(ctx, ins);
                lookup_val_str = NULL;
                lookup_val_len = 0;
            }
        }
        else {
            /* Non-string value: convert to string using two-pass dynamic allocation */
            int required_size = 0;
            
            /* First pass: determine required buffer size */
            switch (rval->type) {
                case FLB_RA_BOOL:
                    required_size = snprintf(NULL, 0, "%s", rval->o.via.boolean ? "true" : "false");
                    break;
                case FLB_RA_INT:
                    required_size = snprintf(NULL, 0, "%" PRId64, rval->o.via.i64);
                    break;
                case FLB_RA_FLOAT:
                    required_size = snprintf(NULL, 0, "%.17g", rval->o.via.f64);
                    break;
                case FLB_RA_NULL:
                    required_size = snprintf(NULL, 0, "null");
                    break;
                case 5:
                case 6:
                    flb_plg_debug(ins, "Record %d: complex type (ARRAY/MAP) from record accessor, skipping lookup", rec_num);
                    INCREMENT_SKIPPED_METRIC(ctx, ins);
                    CLEANUP_DYNAMIC_BUFFERS();
                    flb_ra_key_value_destroy(rval);
                    emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
                    continue;
                default:
                    flb_plg_debug(ins, "Record %d: unsupported type %d, skipping lookup", rec_num, rval->type);
                    INCREMENT_SKIPPED_METRIC(ctx, ins);
                    CLEANUP_DYNAMIC_BUFFERS();
                    flb_ra_key_value_destroy(rval);
                    emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
                    continue;
            }
            
            if (required_size < 0) {
                flb_plg_debug(ins, "Record %d: snprintf sizing failed for type %d, skipping lookup", rec_num, rval->type);
                INCREMENT_SKIPPED_METRIC(ctx, ins);
                CLEANUP_DYNAMIC_BUFFERS();
                flb_ra_key_value_destroy(rval);
                emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
                continue;
            }
            
            /* Allocate buffer with required size plus null terminator */
            dynamic_val_buf = flb_malloc(required_size + 1);
            if (!dynamic_val_buf) {
                flb_plg_warn(ins, "Record %d: malloc failed for dynamic value buffer (size %d), skipping", rec_num, required_size + 1);
                INCREMENT_SKIPPED_METRIC(ctx, ins);
                CLEANUP_DYNAMIC_BUFFERS();
                flb_ra_key_value_destroy(rval);
                emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
                continue;
            }
            
            /* Second pass: write to allocated buffer */
            int printed = 0;
            switch (rval->type) {
                case FLB_RA_BOOL:
                    printed = snprintf(dynamic_val_buf, required_size + 1, "%s", rval->o.via.boolean ? "true" : "false");
                    break;
                case FLB_RA_INT:
                    printed = snprintf(dynamic_val_buf, required_size + 1, "%" PRId64, rval->o.via.i64);
                    break;
                case FLB_RA_FLOAT:
                    printed = snprintf(dynamic_val_buf, required_size + 1, "%.17g", rval->o.via.f64);
                    break;
                case FLB_RA_NULL:
                    printed = snprintf(dynamic_val_buf, required_size + 1, "null");
                    break;
            }
            
            if (printed < 0 || printed != required_size) {
                flb_plg_debug(ins, "Record %d: snprintf formatting failed (expected %d, got %d), skipping lookup", rec_num, required_size, printed);
                INCREMENT_SKIPPED_METRIC(ctx, ins);
                CLEANUP_DYNAMIC_BUFFERS();
                flb_ra_key_value_destroy(rval);
                emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
                continue;
            }
            
            /* Use the dynamically allocated buffer for normalization */
            lookup_val_allocated = normalize_and_trim(dynamic_val_buf, printed, ctx->ignore_case, &lookup_val_str, &lookup_val_len);
            if (lookup_val_allocated < 0) {
                flb_plg_warn(ins, "Record %d: malloc failed for normalize_and_trim (non-string), skipping", rec_num);
                INCREMENT_SKIPPED_METRIC(ctx, ins);
                CLEANUP_DYNAMIC_BUFFERS();
                flb_ra_key_value_destroy(rval);
                emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
                continue;
            }
            
            flb_plg_debug(ins, "Record %d: lookup value for key '%s' is non-string, converted to '%s'", rec_num, ctx->lookup_key, lookup_val_str ? lookup_val_str : "NULL");
            
            /* 
             * If normalize_and_trim allocated a new buffer (lookup_val_allocated > 0), 
             * we can free the dynamic buffer now. Otherwise, lookup_val_str points
             * into dynamic_val_buf and we must delay freeing it.
             */
            if (lookup_val_allocated > 0) {
                flb_free(dynamic_val_buf);
                dynamic_val_buf = NULL;
            }
            /* Note: dynamic_val_buf will be freed later if still allocated */
        }

        /* If lookup value is missing or empty, emit the original record unchanged. */
        if (!lookup_val_str || lookup_val_len == 0) {
            CLEANUP_DYNAMIC_BUFFERS();
            flb_ra_key_value_destroy(rval);
            emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
            continue;
        }

        /*
         * Attempt to find the lookup value in the hash table.
         * If not found, emit the original record unchanged.
         */
        flb_plg_debug(ins, "Record %d: Looking up key '%.*s' (len=%zu)", 
                     rec_num, (int)lookup_val_len, lookup_val_str, lookup_val_len);
        int ht_get_ret = flb_hash_table_get(ctx->ht, lookup_val_str, lookup_val_len, &found_val, &found_len);
        flb_plg_debug(ins, "Record %d: Hash table get returned %d, found_val=%p, found_len=%zu", 
                     rec_num, ht_get_ret, found_val, found_len);
        
        if (ht_get_ret < 0 || !found_val) {
            /* Not found, emit original record */
            CLEANUP_DYNAMIC_BUFFERS();
            flb_ra_key_value_destroy(rval);
            emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
            continue;
        }

        /* Handle hash table bug: when zero-length values are stored, get may return -1 as found_len */
        if (found_len == SIZE_MAX || (found_len > 0 && found_len > 1000000)) {
            /* Likely a hash table bug with zero-length values, treat as empty string */
            found_len = 0;
        }

        /* Match found - increment counter */
        INCREMENT_MATCHED_METRIC(ctx, ins);
        any_modified = true;  /* Mark that we have modified records */
        
        flb_plg_trace(ins, "Record %d: Found match for '%.*s' -> '%.*s'", 
                     rec_num, (int)lookup_val_len, lookup_val_str, (int)found_len, (char*)found_val);
        
        /* Free normalization buffer if allocated (after using it in trace) */
        CLEANUP_DYNAMIC_BUFFERS();
        flb_ra_key_value_destroy(rval);

        /* Begin new record */
        ret = flb_log_event_encoder_begin_record(&log_encoder);
        if (ret != FLB_EVENT_ENCODER_SUCCESS) {
            flb_plg_warn(ins, "Record %d: failed to begin new record, emitting original", rec_num);
            INCREMENT_SKIPPED_METRIC(ctx, ins);
            emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
            continue;
        }

        ret = flb_log_event_encoder_set_timestamp(&log_encoder, &log_event.timestamp);
        if (ret != FLB_EVENT_ENCODER_SUCCESS) {
            flb_plg_warn(ins, "Record %d: failed to set timestamp, emitting original", rec_num);
            INCREMENT_SKIPPED_METRIC(ctx, ins);
            flb_log_event_encoder_rollback_record(&log_encoder);
            emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
            continue;
        }

        if (log_event.metadata) {
            ret = flb_log_event_encoder_set_metadata_from_msgpack_object(&log_encoder, log_event.metadata);
            if (ret != FLB_EVENT_ENCODER_SUCCESS) {
                flb_plg_warn(ins, "Record %d: failed to set metadata, emitting original", rec_num);
                INCREMENT_SKIPPED_METRIC(ctx, ins);
                flb_log_event_encoder_rollback_record(&log_encoder);
                emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
                continue;
            }
        }

        /* Copy all keys except result_key (to avoid collision) */
        if (log_event.body && log_event.body->type == MSGPACK_OBJECT_MAP) {
            int i;
            for (i = 0; i < log_event.body->via.map.size; i++) {
                msgpack_object_kv *kv = &log_event.body->via.map.ptr[i];
                if (kv->key.type == MSGPACK_OBJECT_STR &&
                    kv->key.via.str.size == strlen(ctx->result_key) &&
                    strncmp(kv->key.via.str.ptr, ctx->result_key, kv->key.via.str.size) == 0) {
                    continue;
                }
                ret = flb_log_event_encoder_append_body_values(&log_encoder, 
                    FLB_LOG_EVENT_MSGPACK_OBJECT_VALUE(&kv->key), 
                    FLB_LOG_EVENT_MSGPACK_OBJECT_VALUE(&kv->val));
                if (ret != FLB_EVENT_ENCODER_SUCCESS) {
                    flb_plg_warn(ins, "Record %d: failed to append key/value, emitting original", rec_num);
                    INCREMENT_SKIPPED_METRIC(ctx, ins);
                    flb_log_event_encoder_rollback_record(&log_encoder);
                    emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
                    continue;
                }
            }
        }

        /* Add result_key */
        ret = flb_log_event_encoder_append_body_string(&log_encoder, ctx->result_key, strlen(ctx->result_key));
        if (ret != FLB_EVENT_ENCODER_SUCCESS) {
            flb_plg_warn(ins, "Record %d: failed to append result_key, emitting original", rec_num);
            INCREMENT_SKIPPED_METRIC(ctx, ins);
            flb_log_event_encoder_rollback_record(&log_encoder);
            emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
            continue;
        }

        /* Add found value - handle empty values safely */
        if (found_len > 0 && found_val) {
            ret = flb_log_event_encoder_append_body_string(&log_encoder, (char *)found_val, found_len);
        } else {
            /* For empty or null values, append empty string */
            ret = flb_log_event_encoder_append_body_string(&log_encoder, "", 0);
        }
        if (ret != FLB_EVENT_ENCODER_SUCCESS) {
            flb_plg_warn(ins, "Record %d: failed to append found_val (len=%zu), emitting original", rec_num, found_len);
            INCREMENT_SKIPPED_METRIC(ctx, ins);
            flb_log_event_encoder_rollback_record(&log_encoder);
            emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
            continue;
        }

        ret = flb_log_event_encoder_commit_record(&log_encoder);
        if (ret != FLB_EVENT_ENCODER_SUCCESS) {
            flb_plg_warn(ins, "Record %d: failed to commit record, emitting original", rec_num);
            INCREMENT_SKIPPED_METRIC(ctx, ins);
            flb_log_event_encoder_rollback_record(&log_encoder);
            emit_original_record(&log_encoder, &log_event, ins, ctx, rec_num);
            continue;
        }
    }

    #undef CLEANUP_DYNAMIC_BUFFERS

    /*
     * If any records were modified, return the new buffer.
     * Otherwise, indicate no change to avoid unnecessary buffer copy.
     */
    if (any_modified) {
        *out_buf = log_encoder.output_buffer;
        *out_bytes = log_encoder.output_length;
        flb_log_event_encoder_claim_internal_buffer_ownership(&log_encoder);
        ret = FLB_FILTER_MODIFIED;
    } else {
        ret = FLB_FILTER_NOTOUCH;
    }

    flb_log_event_decoder_destroy(&log_decoder);
    flb_log_event_encoder_destroy(&log_encoder);
    
    /* Release the read lock before returning */
    pthread_rwlock_unlock(&ctx->refresh_rwlock);
    
    return ret;
}

static int cb_lookup_exit(void *data, struct flb_config *config)
{
    struct lookup_ctx *ctx = data;
    if (!ctx) return 0;
    
    /* Destroy refresh timer if active */
    if (ctx->refresh_timer) {
        flb_sched_timer_destroy(ctx->refresh_timer);
        ctx->refresh_timer = NULL;
    }
    
    /* Free all allocated values tracked in val_list */
    cleanup_val_list(&ctx->val_list);
    
    if (ctx->ra_lookup_key) flb_ra_destroy(ctx->ra_lookup_key);
    if (ctx->ht) flb_hash_table_destroy(ctx->ht);
    
    /* Destroy the read-write lock */
    pthread_rwlock_destroy(&ctx->refresh_rwlock);
    
    flb_free(ctx);
    return 0;
}

static struct flb_config_map config_map[] = {
    { FLB_CONFIG_MAP_STR, "file", NULL, 0, FLB_TRUE, offsetof(struct lookup_ctx, file), "CSV file to lookup values from." },
    { FLB_CONFIG_MAP_STR, "lookup_key", NULL, 0, FLB_TRUE, offsetof(struct lookup_ctx, lookup_key), "Name of the key to lookup in input record." },
    { FLB_CONFIG_MAP_STR, "result_key", NULL, 0, FLB_TRUE, offsetof(struct lookup_ctx, result_key), "Name of the key to add to output record if found." },
    { FLB_CONFIG_MAP_STR, "delimiter", ",", 0, FLB_TRUE, offsetof(struct lookup_ctx, delimiter), "Delimiter character for CSV parsing (default: comma)." },
    { FLB_CONFIG_MAP_BOOL, "ignore_case", "false", 0, FLB_TRUE, offsetof(struct lookup_ctx, ignore_case), "Ignore case when matching lookup values (default: false)." },
    { FLB_CONFIG_MAP_INT, "refresh_interval", "0", 0, FLB_TRUE, offsetof(struct lookup_ctx, refresh_interval), "Refresh CSV file every N seconds (0=disabled, default: 0)." },
    { FLB_CONFIG_MAP_INT, "retry_interval", "600", 0, FLB_TRUE, offsetof(struct lookup_ctx, retry_interval), "Retry failed refresh every N seconds (default: 600)." },
    {0}
};

struct flb_filter_plugin filter_lookup_plugin = {
    .name         = "lookup",
    .description  = "Lookup values from CSV file and add to records",
    .cb_init      = cb_lookup_init,
    .cb_filter    = cb_lookup_filter,
    .cb_exit      = cb_lookup_exit,
    .config_map   = config_map,
    .flags        = 0
};
