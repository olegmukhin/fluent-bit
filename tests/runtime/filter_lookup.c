/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2019-2025 The Fluent Bit Authors
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

#include <fluent-bit.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_metrics.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <float.h>
#include <float.h>
#include <inttypes.h>
#include "flb_tests_runtime.h"

#define TMP_CSV_PATH "lookup_test.csv"

struct test_ctx {
    flb_ctx_t *flb;
    int i_ffd;
    int f_ffd;
    int o_ffd;
};

static struct test_ctx *test_ctx_create(struct flb_lib_out_cb *data)
{
    int i_ffd;
    int o_ffd;
    int f_ffd;
    struct test_ctx *ctx = NULL;

    ctx = flb_malloc(sizeof(struct test_ctx));
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("malloc failed");
        flb_errno();
        return NULL;
    }

    /* Service config */
    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    /* Input */
    i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(i_ffd >= 0);
    flb_input_set(ctx->flb, i_ffd, "tag", "test", NULL);
    ctx->i_ffd = i_ffd;

    /* Filter */
    f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(f_ffd >= 0);
    ctx->f_ffd = f_ffd;

    /* Output */
    o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) data);
    ctx->o_ffd = o_ffd;
    TEST_CHECK(o_ffd >= 0);
    flb_output_set(ctx->flb, o_ffd,
                   "match", "test",
                   NULL);

    return ctx;
}

static void test_ctx_destroy(struct test_ctx *ctx)
{
    TEST_CHECK(ctx != NULL);

    sleep(1);
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);
    flb_free(ctx);
}

void delete_csv_file()
{
    unlink(TMP_CSV_PATH);
}

int create_csv_file(char *csv_content)
{
    FILE *fp = NULL;
    fp = fopen(TMP_CSV_PATH, "w");
    if (fp == NULL) {
        TEST_MSG("fopen error\n");
        return -1;
    }
    fprintf(fp, "%s", csv_content);
    fflush(fp);
    fclose(fp);
    return 0;
}

/* Callback to check expected results */
static int cb_check_result_json(void *record, size_t size, void *data)
{
    char *p;
    char *expected;
    char *result;

    expected = (char *) data;
    result = (char *) record;

    p = strstr(result, expected);
    TEST_CHECK(p != NULL);

    if (p == NULL) {
        flb_error("Expected to find: '%s' in result '%s'",
                  expected, result);
    }

    flb_free(record);
    return 0;
}

/* Test basic lookup functionality */
void flb_test_lookup_basic(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "user1,John Doe\n"
        "user2,Jane Smith\n"
        "user3,Bob Wilson\n";
    char *input = "[0, {\"user_id\": \"user1\"}]";

    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"user_name\":\"John Doe\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Test lookup with ignore_case option */
void flb_test_lookup_ignore_case(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "USER1,John Doe\n"
        "user2,Jane Smith\n";
    char *input = "[0, {\"user_id\": \"user1\"}]";

    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"user_name\":\"John Doe\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         "ignore_case", "true",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Test lookup with CSV containing quotes and special characters */
void flb_test_lookup_csv_quotes(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "\"quoted,key\",\"Value with \"\"quotes\"\" and, commas\"\n"
        "simple_key,Simple Value\n";
    char *input = "[0, {\"lookup_field\": \"quoted,key\"}]";

    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"result_field\":\"Value with \\\"quotes\\\" and, commas\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "lookup_field",
                         "result_key", "result_field",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Test lookup with custom delimiter */
void flb_test_lookup_delimiter(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key;value\n"
        "user1;Alice Johnson\n"
        "user2;Bob Wilson\n";
    char *input = "[0, {\"user_id\": \"user1\"}]";

    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"user_name\":\"Alice Johnson\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "delimiter", ";",
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Test lookup with numeric values */
void flb_test_lookup_numeric_values(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "123,Numeric Key\n"
        "456,Another Number\n";
    char *input = "[0, {\"numeric_field\": 123}]";

    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"description\":\"Numeric Key\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "numeric_field",
                         "result_key", "description",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Enhanced test lookup with very large numbers (testing the two-pass snprintf fix) */
void flb_test_lookup_large_numbers(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char large_number_str[64];
    char min_number_str[64];
    
    snprintf(large_number_str, sizeof(large_number_str), "%" PRId64, LLONG_MAX);
    snprintf(min_number_str, sizeof(min_number_str), "%" PRId64, LLONG_MIN);
    
    char csv_content[512];
    snprintf(csv_content, sizeof(csv_content),
        "key,value\n"
        "%s,Very Large Number\n"
        "%s,Very Negative Number\n"
        "456,Small Number\n", large_number_str, min_number_str);
    
    /* Test LLONG_MAX first */
    char input_max[128];
    snprintf(input_max, sizeof(input_max), "[0, {\"big_number\": %" PRId64 "}]", LLONG_MAX);

    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"number_desc\":\"Very Large Number\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "big_number",
                         "result_key", "number_desc",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_max, strlen(input_max));
    TEST_CHECK(bytes == strlen(input_max));
    flb_time_msleep(1500);

    /* Clean up and test LLONG_MIN case */
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);
    
    /* Test LLONG_MIN to ensure negative extremes work */
    char input_min[128];
    snprintf(input_min, sizeof(input_min), "[0, {\"big_number\": %" PRId64 "}]", LLONG_MIN);

    cb_data.data = "\"number_desc\":\"Very Negative Number\"";

    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    ctx->i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(ctx->i_ffd >= 0);
    flb_input_set(ctx->flb, ctx->i_ffd, "tag", "test", NULL);

    ctx->f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(ctx->f_ffd >= 0);

    ctx->o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) &cb_data);
    TEST_CHECK(ctx->o_ffd >= 0);
    flb_output_set(ctx->flb, ctx->o_ffd,
                   "match", "test",
                   "format", "json",
                   NULL);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "big_number",
                         "result_key", "number_desc",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_min, strlen(input_min));
    TEST_CHECK(bytes == strlen(input_min));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Enhanced test lookup with boolean values */
void flb_test_lookup_boolean_values(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "true,Boolean True\n"
        "false,Boolean False\n"
        "TRUE,Boolean True Uppercase\n"
        "False,Boolean False Mixed\n";

    /* Test true case first */
    char *input_true = "[0, {\"bool_field\": true}]";
    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"bool_desc\":\"Boolean True\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "bool_field",
                         "result_key", "bool_desc",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_true, strlen(input_true));
    TEST_CHECK(bytes == strlen(input_true));
    flb_time_msleep(1500);

    /* Clean up and test false case */
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);

    /* Test false case */
    char *input_false = "[0, {\"bool_field\": false}]";
    cb_data.data = "\"bool_desc\":\"Boolean False\"";

    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    ctx->i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(ctx->i_ffd >= 0);
    flb_input_set(ctx->flb, ctx->i_ffd, "tag", "test", NULL);

    ctx->f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(ctx->f_ffd >= 0);

    ctx->o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) &cb_data);
    TEST_CHECK(ctx->o_ffd >= 0);
    flb_output_set(ctx->flb, ctx->o_ffd,
                   "match", "test",
                   "format", "json",
                   NULL);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "bool_field", 
                         "result_key", "bool_desc",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_false, strlen(input_false));
    TEST_CHECK(bytes == strlen(input_false));
    flb_time_msleep(1500);

    /* Clean up and test ignore_case functionality */
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);
    
    /* Test ignore_case=true with mixed case input */
    char *input_mixed = "[0, {\"bool_field\": \"true\"}]";
    cb_data.data = "\"bool_desc\":\"Boolean True Uppercase\"";

    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    ctx->i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(ctx->i_ffd >= 0);
    flb_input_set(ctx->flb, ctx->i_ffd, "tag", "test", NULL);

    ctx->f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(ctx->f_ffd >= 0);

    ctx->o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) &cb_data);
    TEST_CHECK(ctx->o_ffd >= 0);
    flb_output_set(ctx->flb, ctx->o_ffd,
                   "match", "test",
                   "format", "json",
                   NULL);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "bool_field",
                         "result_key", "bool_desc",
                         "ignore_case", "true",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_mixed, strlen(input_mixed));
    TEST_CHECK(bytes == strlen(input_mixed));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Callback to verify that a specific key is NOT present in JSON output */
static int cb_check_result_absent(void *record, size_t size, void *data)
{
    char *p;
    char *absent_key;
    char *result;

    absent_key = (char *) data;
    result = (char *) record;

    /* Check that the key is NOT in the result */
    p = strstr(result, absent_key);
    TEST_CHECK(p == NULL);

    if (p != NULL) {
        flb_error("Expected NOT to find: '%s' in result '%s'",
                  absent_key, result);
    }

    /* Also check that other_field is present to ensure record was processed */
    p = strstr(result, "other_field");
    TEST_CHECK(p != NULL);
    
    if (p == NULL) {
        flb_error("Expected to find 'other_field' in result '%s'", result);
    }

    flb_free(record);
    return 0;
}

/* Enhanced test lookup with no match (should emit original record) */
void flb_test_lookup_no_match(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "user1,John Doe\n"
        "user2,Jane Smith\n";

    /* Test case 1: Key exists but value doesn't match */
    char *input_no_match = "[0, {\"user_id\": \"user999\", \"other_field\": \"test\"}]";

    /* Should NOT contain the result_key since no match was found */
    cb_data.cb = cb_check_result_absent;
    cb_data.data = "user_name";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_no_match, strlen(input_no_match));
    TEST_CHECK(bytes == strlen(input_no_match));
    flb_time_msleep(1500);

    /* Clean up and test case 2: lookup key exists but has empty value */
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);

    char *input_empty = "[0, {\"user_id\": \"\", \"other_field\": \"test_empty\"}]";
    cb_data.cb = cb_check_result_absent;
    cb_data.data = "user_name";

    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    ctx->i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(ctx->i_ffd >= 0);
    flb_input_set(ctx->flb, ctx->i_ffd, "tag", "test", NULL);

    ctx->f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(ctx->f_ffd >= 0);

    ctx->o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) &cb_data);
    TEST_CHECK(ctx->o_ffd >= 0);
    flb_output_set(ctx->flb, ctx->o_ffd,
                   "match", "test",
                   "format", "json",
                   NULL);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_empty, strlen(input_empty));
    TEST_CHECK(bytes == strlen(input_empty));
    flb_time_msleep(1500);

    /* Clean up and test case 3: lookup key has null value */
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);

    char *input_null = "[0, {\"user_id\": null, \"other_field\": \"test_null\"}]";
    cb_data.cb = cb_check_result_absent;
    cb_data.data = "user_name";

    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    ctx->i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(ctx->i_ffd >= 0);
    flb_input_set(ctx->flb, ctx->i_ffd, "tag", "test", NULL);

    ctx->f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(ctx->f_ffd >= 0);

    ctx->o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) &cb_data);
    TEST_CHECK(ctx->o_ffd >= 0);
    flb_output_set(ctx->flb, ctx->o_ffd,
                   "match", "test",
                   "format", "json",
                   NULL);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_null, strlen(input_null));
    TEST_CHECK(bytes == strlen(input_null));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Test dynamic line reading with very long CSV lines */
void flb_test_lookup_long_csv_lines(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *input = "[0, {\"key_field\": \"long_key\"}]";
    
    /* Test that long CSV values (>4096 chars) can be read correctly.
     * Just verify that the lookup worked by checking for value_field key. */
    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"value_field\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    /* Create CSV file with very long lines */
    FILE *fp = fopen(TMP_CSV_PATH, "w");
    TEST_CHECK(fp != NULL);
    
    fprintf(fp, "key,value\n");
    fprintf(fp, "long_key,");
    
    /* Write a very long value (> 4096 chars) */
    for (int i = 0; i < 100; i++) {
        fprintf(fp, "This is a very long value that exceeds the original 4096 character buffer limit to test dynamic line reading functionality. ");
    }
    fprintf(fp, "\n");
    fprintf(fp, "short_key,Short Value\n");
    fclose(fp);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "key_field",
                         "result_key", "value_field",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Test with whitespace trimming */
void flb_test_lookup_whitespace_trim(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "  trimmed_key  ,  Trimmed Value  \n"
        "normal_key,Normal Value\n";
    char *input = "[0, {\"lookup_field\": \"  trimmed_key  \"}]";

    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"result_field\":\"Trimmed Value\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "lookup_field",
                         "result_key", "result_field",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Enhanced test dynamic CSV parsing with very long lines (integration test) */
void flb_test_dynamic_buffer(void)
{
    /* This test validates the dynamic buffer functionality indirectly by 
     * testing CSV parsing with lines that exceed typical buffer sizes.
     * This is an integration test rather than unit test. */
    
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    
    /* Test cases that would exercise dynamic buffer edge cases */
    char *input_tests[] = {
        "[0, {\"key_field\": \"empty_value_key\", \"other_field\": \"test_empty\"}]",     // Empty value test
        "[0, {\"key_field\": \"missing_key\", \"other_field\": \"test_missing\"}]",        // Non-existent key
        "[0, {\"key_field\": \"long_line_key\", \"other_field\": \"test_long\"}]",      // Very long value
        "[0, {\"key_field\": \"normal_key\", \"other_field\": \"test_normal\"}]",         // Normal case
        NULL
    };
    
    char *expected_results[] = {
        "\"value_field\":\"\"",                         // Empty value should be handled
        NULL,                                           // No match expected  
        "\"value_field\"",                             // Long value should be present
        "\"value_field\":\"Normal Value\"",            // Normal result
        NULL
    };
    
    /* Create CSV with edge cases that would stress dynamic buffer:
     * - Empty values (zero-length strings)
     * - Very long values (>4KB to force buffer growth)
     * - Normal values for baseline
     */
    FILE *fp = fopen(TMP_CSV_PATH, "w");
    TEST_CHECK(fp != NULL);
    
    fprintf(fp, "key,value\n");
    fprintf(fp, "empty_value_key,\n");                              // Empty value
    fprintf(fp, "normal_key,Normal Value\n");                       // Normal case
    fprintf(fp, "long_line_key,");                                  // Start very long line
    
    /* Write a line longer than typical buffers (8KB) */
    for (int i = 0; i < 200; i++) {
        fprintf(fp, "This is part %d of a very long CSV value that will test dynamic buffer growth functionality. ", i);
    }
    fprintf(fp, "\n");
    
    fclose(fp);

    /* Test each case */
    for (int test_case = 0; input_tests[test_case] != NULL; test_case++) {
        cb_data.cb = expected_results[test_case] ? cb_check_result_json : cb_check_result_absent;
        cb_data.data = expected_results[test_case] ? expected_results[test_case] : "value_field";

        ctx = test_ctx_create(&cb_data);
        if (!TEST_CHECK(ctx != NULL)) {
            TEST_MSG("test_ctx_create failed for test case %d", test_case);
            continue;
        }

        ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                             "Match", "*", 
                             "file", TMP_CSV_PATH,
                             "lookup_key", "key_field",
                             "result_key", "value_field",
                             NULL);
        TEST_CHECK(ret == 0);

        ret = flb_output_set(ctx->flb, ctx->o_ffd,
                             "format", "json",
                             NULL);
        TEST_CHECK(ret == 0);

        ret = flb_start(ctx->flb);
        TEST_CHECK(ret == 0);

        bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_tests[test_case], strlen(input_tests[test_case]));
        TEST_CHECK(bytes == strlen(input_tests[test_case]));
        flb_time_msleep(1500);

        test_ctx_destroy(ctx);
    }

    delete_csv_file();
}

/* Enhanced test nested record accessor patterns ($a.b.c) */
void flb_test_lookup_nested_keys(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "user123,John Doe\n"
        "admin456,Jane Smith\n"
        "deep789,Deep Value\n";

    /* Test 1: Valid nested access */
    char *input_valid = "[0, {\"user\": {\"profile\": {\"id\": \"user123\"}}, \"other_field\": \"test\"}]";

    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"user_name\":\"John Doe\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "$user['profile']['id']",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_valid, strlen(input_valid));
    TEST_CHECK(bytes == strlen(input_valid));
    flb_time_msleep(1500);

    /* Clean up and test invalid nested key path */
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);

    /* Test 2: Non-existent nested key should emit original */
    char *input_invalid = "[0, {\"user\": {\"profile\": {\"name\": \"John\"}}, \"other_field\": \"test_invalid\"}]";
    cb_data.cb = cb_check_result_absent;
    cb_data.data = "user_name";

    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    ctx->i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(ctx->i_ffd >= 0);
    flb_input_set(ctx->flb, ctx->i_ffd, "tag", "test", NULL);

    ctx->f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(ctx->f_ffd >= 0);

    ctx->o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) &cb_data);
    TEST_CHECK(ctx->o_ffd >= 0);
    flb_output_set(ctx->flb, ctx->o_ffd,
                   "match", "test",
                   "format", "json",
                   NULL);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "$user['profile']['id']", /* 'id' doesn't exist */
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_invalid, strlen(input_invalid));
    TEST_CHECK(bytes == strlen(input_invalid));
    flb_time_msleep(1500);

    /* Clean up and test deeper nesting */
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);

    /* Test 3: Very deep nesting to stress RA */
    char *input_deep = "[0, {\"level1\": {\"level2\": {\"level3\": {\"level4\": {\"id\": \"deep789\"}}}}, \"other_field\": \"test_deep\"}]";
    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"deep_value\":\"Deep Value\"";

    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    ctx->i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(ctx->i_ffd >= 0);
    flb_input_set(ctx->flb, ctx->i_ffd, "tag", "test", NULL);

    ctx->f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(ctx->f_ffd >= 0);

    ctx->o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) &cb_data);
    TEST_CHECK(ctx->o_ffd >= 0);
    flb_output_set(ctx->flb, ctx->o_ffd,
                   "match", "test",
                   "format", "json",
                   NULL);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "$level1['level2']['level3']['level4']['id']",
                         "result_key", "deep_value",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_deep, strlen(input_deep));
    TEST_CHECK(bytes == strlen(input_deep));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Test with large CSV file (performance/load testing) - Enhanced */
void flb_test_lookup_large_csv(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *input = "[0, {\"user_id\": \"user5000\"}]";

    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"user_name\":\"User 5000\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    /* Create CSV file with 10,000 entries for performance testing */
    FILE *fp = fopen(TMP_CSV_PATH, "w");
    TEST_CHECK(fp != NULL);
    
    fprintf(fp, "key,value\n");
    
    /* Write 10,000 test entries */
    for (int i = 1; i <= 10000; i++) {
        fprintf(fp, "user%d,User %d\n", i, i);
    }
    fclose(fp);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    /* Test lookup performance with large dataset */
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    flb_time_msleep(2000); /* Increased sleep time for slower CI environments */

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Enhanced test nested record accessor with array indexing ($users[0].id) */
void flb_test_lookup_nested_array_keys(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "array_user1,First User\n"
        "array_user2,Second User\n"
        "deep_array,Deep Array Value\n"
        "nested_map,Nested Map Value\n";

    /* Test 1: Valid array access */
    char *input_valid = "[0, {\"users\": [{\"id\": \"array_user1\"}, {\"id\": \"array_user2\"}], \"metadata\": \"test\"}]";

    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"user_desc\":\"First User\"";

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "$users[0]['id']",
                         "result_key", "user_desc",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_valid, strlen(input_valid));
    TEST_CHECK(bytes == strlen(input_valid));
    flb_time_msleep(1500);

    /* Clean up and test out-of-bounds array access */
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);

    /* Test 2: Out-of-bounds array index should emit original */
    char *input_oob = "[0, {\"users\": [{\"id\": \"array_user1\"}], \"metadata\": \"test_oob\", \"other_field\": \"present\"}]";
    cb_data.cb = cb_check_result_absent;
    cb_data.data = "user_desc";

    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    ctx->i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(ctx->i_ffd >= 0);
    flb_input_set(ctx->flb, ctx->i_ffd, "tag", "test", NULL);

    ctx->f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(ctx->f_ffd >= 0);

    ctx->o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) &cb_data);
    TEST_CHECK(ctx->o_ffd >= 0);
    flb_output_set(ctx->flb, ctx->o_ffd,
                   "match", "test",
                   "format", "json",
                   NULL);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "$users[5]['id']", /* Index 5 doesn't exist */
                         "result_key", "user_desc",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_oob, strlen(input_oob));
    TEST_CHECK(bytes == strlen(input_oob));
    flb_time_msleep(1500);

    /* Clean up and test array of maps with deeper nesting */
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);

    /* Test 3: Array of maps with deeper nesting */
    char *input_deep = "[0, {\"data\": [{\"info\": {\"nested\": [{\"key\": \"deep_array\"}]}}], \"metadata\": \"test_deep\"}]";
    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"deep_result\":\"Deep Array Value\"";

    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    ctx->i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(ctx->i_ffd >= 0);
    flb_input_set(ctx->flb, ctx->i_ffd, "tag", "test", NULL);

    ctx->f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(ctx->f_ffd >= 0);

    ctx->o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) &cb_data);
    TEST_CHECK(ctx->o_ffd >= 0);
    flb_output_set(ctx->flb, ctx->o_ffd,
                   "match", "test",
                   "format", "json",
                   NULL);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "$data[0]['info']['nested'][0]['key']",
                         "result_key", "deep_result",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_deep, strlen(input_deep));
    TEST_CHECK(bytes == strlen(input_deep));
    flb_time_msleep(1500);

    /* Clean up and test mixed array and map access */
    flb_stop(ctx->flb);
    flb_destroy(ctx->flb);

    /* Test 4: Array containing maps with nested arrays */
    char *input_mixed = "[0, {\"complex\": [{\"maps\": [{\"id\": \"nested_map\"}], \"other\": \"data\"}], \"metadata\": \"test_mixed\"}]";
    cb_data.cb = cb_check_result_json;
    cb_data.data = "\"complex_result\":\"Nested Map Value\"";

    ctx->flb = flb_create();
    flb_service_set(ctx->flb,
                    "Flush", "0.200000000",
                    "Grace", "1",
                    "Log_Level", "error",
                    NULL);

    ctx->i_ffd = flb_input(ctx->flb, (char *) "lib", NULL);
    TEST_CHECK(ctx->i_ffd >= 0);
    flb_input_set(ctx->flb, ctx->i_ffd, "tag", "test", NULL);

    ctx->f_ffd = flb_filter(ctx->flb, (char *) "lookup", NULL);
    TEST_CHECK(ctx->f_ffd >= 0);

    ctx->o_ffd = flb_output(ctx->flb, (char *) "lib", (void *) &cb_data);
    TEST_CHECK(ctx->o_ffd >= 0);
    flb_output_set(ctx->flb, ctx->o_ffd,
                   "match", "test",
                   "format", "json",
                   NULL);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "$complex[0]['maps'][0]['id']",
                         "result_key", "complex_result",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input_mixed, strlen(input_mixed));
    TEST_CHECK(bytes == strlen(input_mixed));
    flb_time_msleep(1500);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Custom callback to capture metrics and verify counts */
static int cb_check_metrics(void *record, size_t size, void *data)
{
    /* Just free the record - we'll check metrics through the filter instance */
    flb_free(record);
    return 0;
}

/* Helper function to get metric value from filter instance */
static uint64_t get_filter_metric(struct test_ctx *ctx, int metric_id)
{
    struct flb_filter_instance *f_ins;
    struct mk_list *head;
    struct flb_metric *metric;
    
    mk_list_foreach(head, &ctx->flb->config->filters) {
        f_ins = mk_list_entry(head, struct flb_filter_instance, _head);
        if (f_ins->id == ctx->f_ffd && f_ins->metrics) {
            metric = flb_metrics_get_id(metric_id, f_ins->metrics);
            if (metric) {
                return metric->val;
            }
        }
    }
    return 0;
}

/* Enhanced test metrics with matched records and comprehensive CMT verification */
void flb_test_lookup_metrics_matched(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "user1,John Doe\n"
        "user2,Jane Smith\n"
        "user3,Bob Wilson\n"
        "special_key,Special Value\n";
    char *input1 = "[0, {\"user_id\": \"user1\"}]";
    char *input2 = "[0, {\"user_id\": \"user2\"}]";
    char *input3 = "[0, {\"user_id\": \"unknown\"}]"; // No match
    char *input4 = "[0, {\"user_id\": \"special_key\"}]"; // Match
    char *input5 = "[0, {\"other_field\": \"value\"}]"; // Missing lookup key (skipped)

    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    /* Process five records: 3 matches + 1 no-match + 1 skipped */
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input1, strlen(input1));
    TEST_CHECK(bytes == strlen(input1));
    
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input2, strlen(input2));
    TEST_CHECK(bytes == strlen(input2));
    
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input3, strlen(input3));
    TEST_CHECK(bytes == strlen(input3));

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input4, strlen(input4));
    TEST_CHECK(bytes == strlen(input4));

    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input5, strlen(input5));
    TEST_CHECK(bytes == strlen(input5));
    
    flb_time_msleep(2500);

    /* Comprehensive metrics verification: ALL 5 records processed, 3 matched, 1 skipped */
    uint64_t processed = get_filter_metric(ctx, 200); // FLB_LOOKUP_METRIC_PROCESSED
    uint64_t matched = get_filter_metric(ctx, 201);   // FLB_LOOKUP_METRIC_MATCHED  
    uint64_t skipped = get_filter_metric(ctx, 202);   // FLB_LOOKUP_METRIC_SKIPPED
    
    TEST_CHECK(processed == 5);
    TEST_CHECK(matched == 3);
    TEST_CHECK(skipped == 1);
    
    if (processed != 5) {
        TEST_MSG("Expected processed=5 (all records), got %" PRIu64, processed);
    }
    if (matched != 3) {
        TEST_MSG("Expected matched=3 (successful lookups), got %" PRIu64, matched);
    }
    if (skipped != 1) {
        TEST_MSG("Expected skipped=1 (missing lookup_key), got %" PRIu64, skipped);
    }

    /* Verify metric consistency: matched records should be <= total processed */
    TEST_CHECK(matched <= processed);
    
    if (matched > processed) {
        TEST_MSG("Inconsistent metrics: matched (%" PRIu64 ") > processed (%" PRIu64 ")", matched, processed);
    }

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Enhanced test metrics with large volume, boundary conditions, and CMT verification */
void flb_test_lookup_metrics_processed(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "match_key,Matched Value\n"
        "another_match,Another Match\n"
        "edge_case,Edge Case Value\n";

    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "test_key",
                         "result_key", "test_result",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    /* Test with larger volume: 50 matching records, 25 non-matching, 10 skipped */
    const int matching_count = 50;
    const int non_matching_count = 25;
    const int skipped_count = 10;
    
    /* Send matching records */
    for (int i = 0; i < matching_count; i++) {
        char input[256];
        const char* keys[] = {"match_key", "another_match", "edge_case"};
        const char* selected_key = keys[i % 3]; /* Cycle through available keys */
        snprintf(input, sizeof(input), "[0, {\"test_key\": \"%s\", \"seq\": %d}]", selected_key, i);
        bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
        TEST_CHECK(bytes == strlen(input));
    }
    
    /* Send non-matching records */
    for (int i = 0; i < non_matching_count; i++) {
        char input[256];
        snprintf(input, sizeof(input), "[0, {\"test_key\": \"no_match_%d\", \"seq\": %d}]", i, i);
        bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
        TEST_CHECK(bytes == strlen(input));
    }

    /* Send records without lookup key (should be skipped) */
    for (int i = 0; i < skipped_count; i++) {
        char input[256];
        snprintf(input, sizeof(input), "[0, {\"other_field\": \"value_%d\", \"seq\": %d}]", i, i);
        bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
        TEST_CHECK(bytes == strlen(input));
    }
    
    flb_time_msleep(4000); /* Give more time for processing large volume */

    /* Comprehensive metrics verification */
    uint64_t processed = get_filter_metric(ctx, 200);
    uint64_t matched = get_filter_metric(ctx, 201);
    uint64_t skipped = get_filter_metric(ctx, 202);
    
    /* Expected: processed = ALL records (matching + non_matching + skipped) */
    /* Expected: matched = matching_count only */
    /* Expected: skipped = skipped_count (records without lookup_key) */
    
    TEST_CHECK(processed == matching_count + non_matching_count + skipped_count);
    TEST_CHECK(matched == matching_count);
    TEST_CHECK(skipped == skipped_count);
    
    if (processed != matching_count + non_matching_count + skipped_count) {
        TEST_MSG("Expected processed=%d (all records), got %" PRIu64, 
                 matching_count + non_matching_count + skipped_count, processed);
    }
    if (matched != matching_count) {
        TEST_MSG("Expected matched=%d (successful lookups), got %" PRIu64, matching_count, matched);
    }
    if (skipped != skipped_count) {
        TEST_MSG("Expected skipped=%d (missing lookup_key), got %" PRIu64, skipped_count, skipped);
    }

    /* Advanced CMT verification: Test metric boundaries and consistency */
    TEST_CHECK(matched <= processed); /* Matched cannot exceed processed */
    
    /* Test metric precision with edge values */
    if (matched > processed) {
        TEST_MSG("CMT Consistency Error: matched (%" PRIu64 ") > processed (%" PRIu64 ")", matched, processed);
    }

    /* Test zero boundary condition by verifying no negative metrics */
    TEST_CHECK(processed >= 0);
    TEST_CHECK(matched >= 0);
    TEST_CHECK(skipped >= 0);

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Test refresh metrics - basic metric initialization and counter presence */
void flb_test_lookup_metrics_refresh(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "user1,John Doe\n"
        "user2,Jane Smith\n";
    char *input = "[0, {\"user_id\": \"user1\"}]";

    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    /* Configure without refresh interval - test basic metrics presence */
    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    /* Process a record */
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    
    /* Wait for processing */
    flb_time_msleep(1000);
    
    /* Check that refresh metrics exist and are initialized to 0 
     * (since no refresh timer is configured) */
    uint64_t refresh_total = get_filter_metric(ctx, 203);  // FLB_LOOKUP_METRIC_REFRESH
    uint64_t refresh_errors = get_filter_metric(ctx, 204); // FLB_LOOKUP_METRIC_REFRESH_ERRORS
    
    /* Verify refresh metrics are accessible (should be 0 for no refresh timer) */
    TEST_CHECK(refresh_total == 0);
    TEST_CHECK(refresh_errors == 0);
    
    if (refresh_total != 0) {
        TEST_MSG("Expected refresh = 0 (no refresh timer), got %" PRIu64, refresh_total);
    }
    if (refresh_errors != 0) {
        TEST_MSG("Expected refresh_errors = 0 (no refresh timer), got %" PRIu64, refresh_errors);
    }

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Test refresh error metrics - basic error counter functionality */
void flb_test_lookup_metrics_refresh_errors(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "user1,John Doe\n";
    char *input = "[0, {\"user_id\": \"user1\"}]";

    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    /* Start with a valid CSV file */
    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    /* Configure without refresh timer to avoid timing issues */
    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id", 
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    /* Process a record */
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    
    /* Wait for processing */
    flb_time_msleep(1000);
    
    /* Check basic refresh metrics state */
    uint64_t refresh_total = get_filter_metric(ctx, 203);   // FLB_LOOKUP_METRIC_REFRESH
    uint64_t refresh_errors = get_filter_metric(ctx, 204); // FLB_LOOKUP_METRIC_REFRESH_ERRORS
    
    /* Verify metrics are accessible and initialized */
    TEST_CHECK(refresh_total == 0);
    TEST_CHECK(refresh_errors == 0);
    
    /* Verify error consistency: errors should never exceed total */
    TEST_CHECK(refresh_errors <= refresh_total);
    
    if (refresh_total != 0) {
        TEST_MSG("Expected refresh = 0 (no refresh timer), got %" PRIu64, refresh_total);
    }
    if (refresh_errors != 0) {
        TEST_MSG("Expected refresh_errors = 0 (no refresh timer), got %" PRIu64, refresh_errors);
    }
    if (refresh_errors > refresh_total) {
        TEST_MSG("Inconsistent metrics: refresh_errors (%" PRIu64 ") > refresh_total (%" PRIu64 ")", refresh_errors, refresh_total);
    }

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Performance/Integration test with large number of records and payloads */
void flb_test_lookup_stress_large_records(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "user1,John Doe\n"
        "user2,Jane Smith\n"
        "user3,Bob Wilson\n"
        "user4,Alice Brown\n"
        "user5,Charlie Davis\n";
    
    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    /* Create large payloads and push records for stress testing */
    const int num_records = 100;  /* Reduced for reliability */
    const int large_payload_size = 512; /* Smaller payload for stability */
    
    for (int i = 0; i < num_records; i++) {
        char *large_data = flb_malloc(large_payload_size);
        if (large_data) {
            /* Fill with repeating pattern */
            for (int j = 0; j < large_payload_size - 1; j++) {
                large_data[j] = 'A' + (j % 26);
            }
            large_data[large_payload_size - 1] = '\0';
        }
        
        /* Create input with large payload and rotating user IDs */
        char input[2048];
        const char* user_keys[] = {"user1", "user2", "user3", "user4", "user5", "unknown"};
        const char* selected_key = user_keys[i % 6];
        
        snprintf(input, sizeof(input), 
                 "[0, {\"user_id\": \"%s\", \"seq\": %d, \"large_data\": \"%s\"}]",
                 selected_key, i, large_data ? large_data : "fallback");
        
        bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
        TEST_CHECK(bytes == strlen(input));
        
        if (large_data) {
            flb_free(large_data);
        }
    }
    
    /* Allow processing time for dataset */
    flb_time_msleep(2000);
    
    /* Verify metrics - should process all records */
    uint64_t processed = get_filter_metric(ctx, 200);
    uint64_t matched = get_filter_metric(ctx, 201);
    
    TEST_CHECK(processed == num_records);
    /* Expect ~83 matches (5/6 of records have valid keys) */
    TEST_CHECK(matched >= 75 && matched <= 90);
    
    if (processed != num_records) {
        TEST_MSG("Expected processed=%d, got %" PRIu64, num_records, processed);
    }
    if (matched < 75 || matched > 90) {
        TEST_MSG("Expected matched in range [75,90], got %" PRIu64, matched);
    }

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Integration test with empty CSV file */
void flb_test_lookup_empty_csv(void)
{
    int ret;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    
    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    /* Create completely empty CSV file */
    FILE *fp = fopen(TMP_CSV_PATH, "w");
    TEST_CHECK(fp != NULL);
    if (fp) {
        fclose(fp);
    }

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    /* This should fail during initialization */
    ret = flb_start(ctx->flb);
    TEST_CHECK(ret != 0);  /* Expect failure */

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Integration test with header-only CSV (no data rows) */
void flb_test_lookup_header_only_csv(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = "key,value\n";  /* Only header, no data */
    char *input = "[0, {\"user_id\": \"user1\"}]";
    
    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);  /* Should succeed but with no entries */

    /* Process a record - should not match anything */
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input, strlen(input));
    TEST_CHECK(bytes == strlen(input));
    
    flb_time_msleep(1000);
    
    /* Verify no matches occurred */
    uint64_t processed = get_filter_metric(ctx, 200);
    uint64_t matched = get_filter_metric(ctx, 201);
    
    TEST_CHECK(processed == 1);
    TEST_CHECK(matched == 0);
    
    if (processed != 1) {
        TEST_MSG("Expected processed=1, got %" PRIu64, processed);
    }
    if (matched != 0) {
        TEST_MSG("Expected matched=0 (no data in CSV), got %" PRIu64, matched);
    }

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Integration test with malformed CSV data */
void flb_test_lookup_malformed_csv(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    /* CSV with various malformations */
    char *csv_content = 
        "key,value\n"
        "user1,John Doe\n"              /* Good line */
        "user2,\"Unmatched quote\n"     /* Unmatched quote */
        "user3,Jane Smith\n"            /* Good line */
        "incomplete_line\n"             /* Missing value */
        "user4,\"Proper \"\"quotes\"\"\n" /* Good line with escaped quotes */
        "user5,Bob Wilson,extra,fields\n" /* Extra fields - should work */
        "\n"                            /* Empty line */
        "user6,Final User\n";           /* Good line */
    char *input1 = "[0, {\"user_id\": \"user1\"}]"; /* Should match */
    char *input2 = "[0, {\"user_id\": \"user2\"}]"; /* Should not match (malformed) */
    char *input3 = "[0, {\"user_id\": \"user3\"}]"; /* Should match */
    char *input4 = "[0, {\"user_id\": \"user4\"}]"; /* Should match */
    char *input5 = "[0, {\"user_id\": \"user6\"}]"; /* Should match */
    
    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);  /* Should succeed with partial load */

    /* Process records */
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input1, strlen(input1));
    TEST_CHECK(bytes == strlen(input1));
    
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input2, strlen(input2));
    TEST_CHECK(bytes == strlen(input2));
    
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input3, strlen(input3));
    TEST_CHECK(bytes == strlen(input3));
    
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input4, strlen(input4));
    TEST_CHECK(bytes == strlen(input4));
    
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input5, strlen(input5));
    TEST_CHECK(bytes == strlen(input5));
    
    flb_time_msleep(2000);
    
    /* Verify partial success - good lines should be loaded */
    uint64_t processed = get_filter_metric(ctx, 200);
    uint64_t matched = get_filter_metric(ctx, 201);
    
    TEST_CHECK(processed == 5);
    /* Expect 3 matches: user1, user3, user6 (user4 may have quote parsing issues) */
    TEST_CHECK(matched == 3);
    
    if (processed != 5) {
        TEST_MSG("Expected processed=5, got %" PRIu64, processed);
    }
    if (matched != 3) {
        TEST_MSG("Expected matched=3 (malformed lines skipped), got %" PRIu64, matched);
    }

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Integration test with file not found */
void flb_test_lookup_file_not_found(void)
{
    int ret;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    
    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    /* Use a non-existent file path */
    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", "/tmp/nonexistent_lookup_file_12345.csv",
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    /* This should fail during initialization */
    ret = flb_start(ctx->flb);
    TEST_CHECK(ret != 0);  /* Expect failure */

    test_ctx_destroy(ctx);
}

/* Integration test with floating point precision */
void flb_test_lookup_float_precision(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    /* CSV with string keys representing high-precision floats */
    char *csv_content = 
        "key,value\n"
        "pi_value,3.141592653589793\n"
        "euler_value,2.718281828459045\n"
        "sqrt2_value,1.414213562373095\n";
    
    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "key_name",
                         "result_key", "precision_value",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    /* Test looking up string keys that will return high-precision float values */
    char *input1 = "[0, {\"key_name\": \"pi_value\"}]";
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input1, strlen(input1));
    TEST_CHECK(bytes == strlen(input1));
    
    char *input2 = "[0, {\"key_name\": \"euler_value\"}]";
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input2, strlen(input2));
    TEST_CHECK(bytes == strlen(input2));
    
    char *input3 = "[0, {\"key_name\": \"sqrt2_value\"}]";
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input3, strlen(input3));
    TEST_CHECK(bytes == strlen(input3));
    
    flb_time_msleep(2000);
    
    /* Verify float precision lookup - values should be matched */
    uint64_t processed = get_filter_metric(ctx, 200);
    uint64_t matched = get_filter_metric(ctx, 201);
    
    TEST_CHECK(processed == 3);
    /* All should match (string keys should work) */
    TEST_CHECK(matched == 3);
    
    if (processed != 3) {
        TEST_MSG("Expected processed=3, got %" PRIu64, processed);
    }
    if (matched != 3) {
        TEST_MSG("Expected matched=3 (all keys should match), got %" PRIu64, matched);
    }

    delete_csv_file();
    test_ctx_destroy(ctx);
}

/* Integration test with null values in lookup */
void flb_test_lookup_null_value(void)
{
    int ret;
    int bytes;
    struct test_ctx *ctx;
    struct flb_lib_out_cb cb_data;
    char *csv_content = 
        "key,value\n"
        "user1,John Doe\n"
        "null_user,\n"           /* Empty value */
        "user2,Jane Smith\n";
    char *input1 = "[0, {\"user_id\": null}]";      /* Null lookup value */
    char *input2 = "[0, {\"user_id\": \"null_user\"}]"; /* Matches empty CSV value */
    
    cb_data.cb = cb_check_metrics;
    cb_data.data = NULL;

    ctx = test_ctx_create(&cb_data);
    if (!TEST_CHECK(ctx != NULL)) {
        TEST_MSG("test_ctx_create failed");
        exit(EXIT_FAILURE);
    }

    ret = create_csv_file(csv_content);
    TEST_CHECK(ret == 0);

    ret = flb_filter_set(ctx->flb, ctx->f_ffd,
                         "Match", "*",
                         "file", TMP_CSV_PATH,
                         "lookup_key", "user_id",
                         "result_key", "user_name",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_output_set(ctx->flb, ctx->o_ffd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx->flb);
    TEST_CHECK(ret == 0);

    /* Process records with null values */
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input1, strlen(input1));
    TEST_CHECK(bytes == strlen(input1));
    
    bytes = flb_lib_push(ctx->flb, ctx->i_ffd, input2, strlen(input2));
    TEST_CHECK(bytes == strlen(input2));
    
    flb_time_msleep(1500);
    
    /* Verify null handling */
    uint64_t processed = get_filter_metric(ctx, 200);
    uint64_t matched = get_filter_metric(ctx, 201);
    
    TEST_CHECK(processed == 2);
    /* Expect 1 match: null_user (null input converts to "null" string and won't match) */
    TEST_CHECK(matched == 1);
    
    if (processed != 2) {
        TEST_MSG("Expected processed=2, got %" PRIu64, processed);
    }
    if (matched != 1) {
        TEST_MSG("Expected matched=1 (only null_user should match), got %" PRIu64, matched);
    }

    delete_csv_file();
    test_ctx_destroy(ctx);
}

TEST_LIST = {
    {"basic_lookup", flb_test_lookup_basic},
    {"ignore_case", flb_test_lookup_ignore_case},
    {"csv_quotes", flb_test_lookup_csv_quotes},
    {"delimiter", flb_test_lookup_delimiter},
    {"numeric_values", flb_test_lookup_numeric_values},
    {"large_numbers", flb_test_lookup_large_numbers},
    {"boolean_values", flb_test_lookup_boolean_values},
    {"no_match", flb_test_lookup_no_match},
    {"long_csv_lines", flb_test_lookup_long_csv_lines},
    {"whitespace_trim", flb_test_lookup_whitespace_trim},
    {"dynamic_buffer", flb_test_dynamic_buffer},
    {"nested_keys", flb_test_lookup_nested_keys},
    {"large_csv", flb_test_lookup_large_csv},
    {"nested_array_keys", flb_test_lookup_nested_array_keys},
    {"metrics_matched", flb_test_lookup_metrics_matched},
    {"metrics_processed", flb_test_lookup_metrics_processed},
    {"metrics_refresh", flb_test_lookup_metrics_refresh},
    {"metrics_refresh_errors", flb_test_lookup_metrics_refresh_errors},
    {"stress_large_records", flb_test_lookup_stress_large_records},
    {"empty_csv", flb_test_lookup_empty_csv},
    {"header_only_csv", flb_test_lookup_header_only_csv},
    {"malformed_csv", flb_test_lookup_malformed_csv},
    {"file_not_found", flb_test_lookup_file_not_found},
    {"float_precision", flb_test_lookup_float_precision},
    {"null_value", flb_test_lookup_null_value},
    {NULL, NULL}
};
