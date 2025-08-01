macro(DEFINE_OPTION option_name description default_value)
    set(temp_value ${default_value})
    if(FLB_MINIMAL)
        set(temp_value OFF)
    endif()
    option(${option_name} "${description}" ${temp_value})
endmacro()

# Add the FLB_MINIMAL option
option(FLB_MINIMAL "Enable minimal build configuration" No)

# Inputs (sources, data collectors)
# =================================
DEFINE_OPTION(FLB_IN_BLOB                     "Enable Blob input plugin"                     ON)
DEFINE_OPTION(FLB_IN_CALYPTIA_FLEET           "Enable Calyptia Fleet input plugin"           ON)
DEFINE_OPTION(FLB_IN_COLLECTD                 "Enable Collectd input plugin"                 ON)
DEFINE_OPTION(FLB_IN_CPU                      "Enable CPU input plugin"                      ON)
DEFINE_OPTION(FLB_IN_DISK                     "Enable Disk input plugin"                     ON)
DEFINE_OPTION(FLB_IN_DOCKER                   "Enable Docker input plugin"                   ON)
DEFINE_OPTION(FLB_IN_DOCKER_EVENTS            "Enable Docker events input plugin"            ON)
DEFINE_OPTION(FLB_IN_DUMMY                    "Enable Dummy input plugin"                    ON)
DEFINE_OPTION(FLB_IN_ELASTICSEARCH            "Enable Elasticsearch (Bulk API) input plugin" ON)
DEFINE_OPTION(FLB_IN_EMITTER                  "Enable emitter input plugin"                  ON)
DEFINE_OPTION(FLB_IN_EVENT_TEST               "Enable event test plugin"                     OFF)
DEFINE_OPTION(FLB_IN_EVENT_TYPE               "Enable event type plugin"                     ON)
DEFINE_OPTION(FLB_IN_EXEC                     "Enable Exec input plugin"                     ON)
DEFINE_OPTION(FLB_IN_EXEC_WASI                "Enable Exec WASI input plugin"                ON)
DEFINE_OPTION(FLB_IN_FLUENTBIT_METRICS        "Enable Fluent Bit metrics plugin"             ON)
DEFINE_OPTION(FLB_IN_FORWARD                  "Enable Forward input plugin"                  ON)
DEFINE_OPTION(FLB_IN_HEAD                     "Enable Head input plugin"                     ON)
DEFINE_OPTION(FLB_IN_HEALTH                   "Enable Health input plugin"                   ON)
DEFINE_OPTION(FLB_IN_HTTP                     "Enable HTTP input plugin"                     ON)
DEFINE_OPTION(FLB_IN_KAFKA                    "Enable Kafka input plugin"                    ON)
DEFINE_OPTION(FLB_IN_KMSG                     "Enable Kernel log input plugin"               ON)
DEFINE_OPTION(FLB_IN_KUBERNETES_EVENTS        "Enable Kubernetes Events plugin"              ON)
DEFINE_OPTION(FLB_IN_LIB                      "Enable library mode input plugin"             ON)
DEFINE_OPTION(FLB_IN_MEM                      "Enable Memory input plugin"                   ON)
DEFINE_OPTION(FLB_IN_MQTT                     "Enable MQTT Broker input plugin"              ON)
DEFINE_OPTION(FLB_IN_NETIF                    "Enable NetworkIF input plugin"                ON)
DEFINE_OPTION(FLB_IN_NGINX_EXPORTER_METRICS   "Enable Nginx Metrics input plugin"            ON)
DEFINE_OPTION(FLB_IN_NODE_EXPORTER_METRICS    "Enable node exporter metrics input plugin"    ON)
DEFINE_OPTION(FLB_IN_OPENTELEMETRY            "Enable OpenTelemetry input plugin"            ON)
DEFINE_OPTION(FLB_IN_PODMAN_METRICS           "Enable Podman Metrics input plugin"           ON)
DEFINE_OPTION(FLB_IN_PROCESS_EXPORTER_METRICS "Enable process exporter metrics input plugin" ON)
DEFINE_OPTION(FLB_IN_PROC                     "Enable Process input plugin"                  ON)
DEFINE_OPTION(FLB_IN_PROMETHEUS_REMOTE_WRITE  "Enable prometheus remote write input plugin"  ON)
DEFINE_OPTION(FLB_IN_PROMETHEUS_SCRAPE        "Enable Prometheus Scrape input plugin"        ON)
DEFINE_OPTION(FLB_IN_RANDOM                   "Enable random input plugin"                   ON)
DEFINE_OPTION(FLB_IN_SERIAL                   "Enable Serial input plugin"                   ON)
DEFINE_OPTION(FLB_IN_SPLUNK                   "Enable Splunk HTTP HEC input plugin"          ON)
DEFINE_OPTION(FLB_IN_STATSD                   "Enable StatsD input plugin"                   ON)
DEFINE_OPTION(FLB_IN_STDIN                    "Enable Standard input plugin"                 ON)
DEFINE_OPTION(FLB_IN_STORAGE_BACKLOG          "Enable storage backlog input plugin"          ON)
DEFINE_OPTION(FLB_IN_SYSLOG                   "Enable Syslog input plugin"                   ON)
DEFINE_OPTION(FLB_IN_SYSTEMD                  "Enable Systemd input plugin"                  ON)
DEFINE_OPTION(FLB_IN_TAIL                     "Enable Tail input plugin"                     ON)
DEFINE_OPTION(FLB_IN_TCP                      "Enable TCP input plugin"                      ON)
DEFINE_OPTION(FLB_IN_THERMAL                  "Enable Thermal plugin"                        ON)
DEFINE_OPTION(FLB_IN_UDP                      "Enable UDP input plugin"                      ON)
DEFINE_OPTION(FLB_IN_UNIX_SOCKET              "Enable Unix socket input plugin"              OFF)
DEFINE_OPTION(FLB_IN_WINLOG                   "Enable Windows Log input plugin"              OFF)
DEFINE_OPTION(FLB_IN_WINDOWS_EXPORTER_METRICS "Enable windows exporter metrics input plugin" ON)
DEFINE_OPTION(FLB_IN_WINEVTLOG                "Enable Windows EvtLog input plugin"           OFF)
DEFINE_OPTION(FLB_IN_WINSTAT                  "Enable Windows Stat input plugin"             OFF)
DEFINE_OPTION(FLB_IN_EBPF                     "Enable Linux eBPF input plugin"               OFF)

# Processors
# ==========
DEFINE_OPTION(FLB_PROCESSOR_CONTENT_MODIFIER  "Enable content modifier processor"            ON)
DEFINE_OPTION(FLB_PROCESSOR_LABELS            "Enable metrics label manipulation processor"  ON)
DEFINE_OPTION(FLB_PROCESSOR_METRICS_SELECTOR  "Enable metrics selector processor"            ON)
DEFINE_OPTION(FLB_PROCESSOR_OPENTELEMETRY_ENVELOPE "Enable OpenTelemetry envelope processor" ON)
DEFINE_OPTION(FLB_PROCESSOR_SQL               "Enable SQL processor"                         ON)
DEFINE_OPTION(FLB_PROCESSOR_SAMPLING          "Enable sampling processor"                    ON)

# Filters
# =======
DEFINE_OPTION(FLB_FILTER_ALTER_SIZE           "Enable alter_size filter"                     ON)
DEFINE_OPTION(FLB_FILTER_AWS                  "Enable aws filter"                            ON)
DEFINE_OPTION(FLB_FILTER_CHECKLIST            "Enable checklist filter"                      ON)
DEFINE_OPTION(FLB_FILTER_ECS                  "Enable AWS ECS filter"                        ON)
DEFINE_OPTION(FLB_FILTER_EXPECT               "Enable expect filter"                         ON)
DEFINE_OPTION(FLB_FILTER_GEOIP2               "Enable geoip2 filter"                         ON)
DEFINE_OPTION(FLB_FILTER_GREP                 "Enable grep filter"                           ON)
DEFINE_OPTION(FLB_FILTER_KUBERNETES           "Enable kubernetes filter"                     ON)
DEFINE_OPTION(FLB_FILTER_LOG_TO_METRICS       "Enable log-derived metrics filter"            ON)
DEFINE_OPTION(FLB_FILTER_LOOKUP              "Enable lookup filter"                         ON)
DEFINE_OPTION(FLB_FILTER_LUA                  "Enable Lua scripting filter"                  ON)
DEFINE_OPTION(FLB_FILTER_MODIFY               "Enable modify filter"                         ON)
DEFINE_OPTION(FLB_FILTER_MULTILINE            "Enable multiline filter"                      ON)
DEFINE_OPTION(FLB_FILTER_NEST                 "Enable nest filter"                           ON)
DEFINE_OPTION(FLB_FILTER_NIGHTFALL            "Enable Nightfall filter"                      ON)
DEFINE_OPTION(FLB_FILTER_PARSER               "Enable parser filter"                         ON)
DEFINE_OPTION(FLB_FILTER_RECORD_MODIFIER      "Enable record_modifier filter"                ON)
DEFINE_OPTION(FLB_FILTER_REWRITE_TAG          "Enable tag rewrite filter"                    ON)
DEFINE_OPTION(FLB_FILTER_STDOUT               "Enable stdout filter"                         ON)
DEFINE_OPTION(FLB_FILTER_SYSINFO              "Enable sysinfo filter"                        ON)
DEFINE_OPTION(FLB_FILTER_THROTTLE             "Enable throttle filter"                       ON)
DEFINE_OPTION(FLB_FILTER_THROTTLE_SIZE        "Enable throttle size filter"                  OFF)
DEFINE_OPTION(FLB_FILTER_TYPE_CONVERTER       "Enable type converter filter"                 ON)
DEFINE_OPTION(FLB_FILTER_TENSORFLOW           "Enable tensorflow filter"                     OFF)
DEFINE_OPTION(FLB_FILTER_WASM                 "Enable WASM filter"                           ON)

# Outputs (destinations)
# ======================
DEFINE_OPTION(FLB_OUT_AZURE                   "Enable Azure output plugin"                   ON)
DEFINE_OPTION(FLB_OUT_AZURE_BLOB              "Enable Azure output plugin"                   ON)
DEFINE_OPTION(FLB_OUT_AZURE_KUSTO             "Enable Azure Kusto output plugin"             ON)
DEFINE_OPTION(FLB_OUT_AZURE_LOGS_INGESTION    "Enable Azure Logs Ingestion output plugin"    ON)
DEFINE_OPTION(FLB_OUT_BIGQUERY                "Enable BigQuery output plugin"                ON)
DEFINE_OPTION(FLB_OUT_CALYPTIA                "Enable Calyptia monitoring plugin"            ON)
DEFINE_OPTION(FLB_OUT_CHRONICLE               "Enable Google Chronicle output plugin"        ON)
DEFINE_OPTION(FLB_OUT_CLOUDWATCH_LOGS         "Enable AWS CloudWatch output plugin"          ON)
DEFINE_OPTION(FLB_OUT_COUNTER                 "Enable Counter output plugin"                 ON)
DEFINE_OPTION(FLB_OUT_DATADOG                 "Enable DataDog output plugin"                 ON)
DEFINE_OPTION(FLB_OUT_ES                      "Enable Elasticsearch output plugin"           ON)
DEFINE_OPTION(FLB_OUT_EXIT                    "Enable Exit output plugin"                    ON)
DEFINE_OPTION(FLB_OUT_FILE                    "Enable file output plugin"                    ON)
DEFINE_OPTION(FLB_OUT_FLOWCOUNTER             "Enable flowcount output plugin"               ON)
DEFINE_OPTION(FLB_OUT_FORWARD                 "Enable Forward output plugin"                 ON)
DEFINE_OPTION(FLB_OUT_GELF                    "Enable GELF output plugin"                    ON)
DEFINE_OPTION(FLB_OUT_HTTP                    "Enable HTTP output plugin"                    ON)
DEFINE_OPTION(FLB_OUT_INFLUXDB                "Enable InfluxDB output plugin"                ON)
DEFINE_OPTION(FLB_OUT_KAFKA                   "Enable Kafka output plugin"                   ON)
DEFINE_OPTION(FLB_OUT_KAFKA_REST              "Enable Kafka Rest output plugin"              ON)
DEFINE_OPTION(FLB_OUT_KINESIS_FIREHOSE        "Enable AWS Firehose output plugin"            ON)
DEFINE_OPTION(FLB_OUT_KINESIS_STREAMS         "Enable AWS Kinesis output plugin"             ON)
DEFINE_OPTION(FLB_OUT_LIB                     "Enable library mode output plugin"            ON)
DEFINE_OPTION(FLB_OUT_LOGDNA                  "Enable LogDNA output plugin"                  ON)
DEFINE_OPTION(FLB_OUT_LOKI                    "Enable Loki output plugin"                    ON)
DEFINE_OPTION(FLB_OUT_NATS                    "Enable NATS output plugin"                    ON)
DEFINE_OPTION(FLB_OUT_NRLOGS                  "Enable New Relic output plugin"               ON)
DEFINE_OPTION(FLB_OUT_NULL                    "Enable dev null output plugin"                ON)
DEFINE_OPTION(FLB_OUT_OPENSEARCH              "Enable OpenSearch output plugin"              ON)
DEFINE_OPTION(FLB_OUT_OPENTELEMETRY           "Enable OpenTelemetry plugin"                  ON)
DEFINE_OPTION(FLB_OUT_ORACLE_LOG_ANALYTICS    "Enable Oracle Cloud Infrastructure Logging analytics plugin" ON)
DEFINE_OPTION(FLB_OUT_PGSQL                   "Enable PostgreSQL output plugin"              OFF)
DEFINE_OPTION(FLB_OUT_PLOT                    "Enable Plot output plugin"                    ON)
DEFINE_OPTION(FLB_OUT_PROMETHEUS_EXPORTER     "Enable Prometheus exporter plugin"            ON)
DEFINE_OPTION(FLB_OUT_PROMETHEUS_REMOTE_WRITE "Enable Prometheus remote write plugin"        ON)
DEFINE_OPTION(FLB_OUT_RETRY                   "Enable Retry test output plugin"              OFF)
DEFINE_OPTION(FLB_OUT_S3                      "Enable AWS S3 output plugin"                  ON)
DEFINE_OPTION(FLB_OUT_SKYWALKING              "Enable Apache SkyWalking output plugin"       ON)
DEFINE_OPTION(FLB_OUT_SLACK                   "Enable Slack output plugin"                   ON)
DEFINE_OPTION(FLB_OUT_SPLUNK                  "Enable Splunk output plugin"                  ON)
DEFINE_OPTION(FLB_OUT_STACKDRIVER             "Enable Stackdriver output plugin"             ON)
DEFINE_OPTION(FLB_OUT_STDOUT                  "Enable STDOUT output plugin"                  ON)
DEFINE_OPTION(FLB_OUT_SYSLOG                  "Enable Syslog output plugin"                  ON)
DEFINE_OPTION(FLB_OUT_TD                      "Enable Treasure Data output plugin"           ON)
DEFINE_OPTION(FLB_OUT_TCP                     "Enable TCP output plugin"                     ON)
DEFINE_OPTION(FLB_OUT_UDP                     "Enable UDP output plugin"                     ON)
DEFINE_OPTION(FLB_OUT_VIVO_EXPORTER           "Enable Vivo exporter output plugin"           ON)
DEFINE_OPTION(FLB_OUT_WEBSOCKET               "Enable Websocket output plugin"               ON)
