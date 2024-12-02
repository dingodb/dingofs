cc_import(
    name = "libopentelemetry_version",
    static_library = "lib/libopentelemetry_version.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_common",
    static_library = "lib/libopentelemetry_common.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_proto",
    static_library = "lib/libopentelemetry_proto.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_resources",
    static_library = "lib/libopentelemetry_resources.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_http_client_curl",
    static_library = "lib/libopentelemetry_http_client_curl.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_trace",
    static_library = "lib/libopentelemetry_trace.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_logs",
    static_library = "lib/libopentelemetry_logs.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_exporter_otlp_http_client",
    static_library = "lib/libopentelemetry_exporter_otlp_http_client.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_metrics",
    static_library = "lib/libopentelemetry_metrics.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_exporter_ostream_logs",
    static_library = "lib/libopentelemetry_exporter_ostream_logs.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_exporter_ostream_span",
    static_library = "lib/libopentelemetry_exporter_ostream_span.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_exporter_in_memory",
    static_library = "lib/libopentelemetry_exporter_in_memory.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_otlp_recordable",
    static_library = "lib/libopentelemetry_otlp_recordable.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_exporter_ostream_metrics",
    static_library = "lib/libopentelemetry_exporter_ostream_metrics.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_exporter_in_memory_metric",
    static_library = "lib/libopentelemetry_exporter_in_memory_metric.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_exporter_otlp_http_metric",
    static_library = "lib/libopentelemetry_exporter_otlp_http_metric.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_exporter_otlp_http",
    static_library = "lib/libopentelemetry_exporter_otlp_http.a",
    visibility = ["//visibility:private"], 
)

cc_import(
    name = "libopentelemetry_exporter_otlp_http_log",
    static_library = "lib/libopentelemetry_exporter_otlp_http_log.a",
    visibility = ["//visibility:private"], 
)

# NOTE: sequence is important,  please keep the order
cc_library(
    name = "opentelemetry",
    visibility = ["//visibility:public"],
    deps=[
        ":libopentelemetry_version",
        ":libopentelemetry_exporter_in_memory",
        ":libopentelemetry_exporter_otlp_http",
        ":libopentelemetry_exporter_otlp_http_log",
        ":libopentelemetry_exporter_otlp_http_metric",
        ":libopentelemetry_otlp_recordable",
        ":libopentelemetry_exporter_otlp_http_client",
        ":libopentelemetry_proto",
        ":libopentelemetry_exporter_ostream_logs",
        ":libopentelemetry_logs",
        ":libopentelemetry_exporter_ostream_metrics",
        ":libopentelemetry_metrics",
        ":libopentelemetry_exporter_ostream_span",
        ":libopentelemetry_trace",
        ":libopentelemetry_resources",
        ":libopentelemetry_http_client_curl",
        ":libopentelemetry_common",
        ":libopentelemetry_exporter_in_memory_metric",
        "@third-party-headers//:headers",
        "@curl//:curl",
        "@openssl//:openssl",
        "@zlib//:zlib",
        "@protobuf//:protobuf",
    ],
)

