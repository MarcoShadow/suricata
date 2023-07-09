#include "suricata-common.h"
#include "util-log-kafka.h"
#include "util-logopenfile.h"

#ifdef HAVE_LIBRDKAFKA

#define KAFKA_ERR_MSG_LEN   256

static int SCConfLogOpenKafkaTopic(ConfNode *conf, Kafka *kafka)
{
    const char *topic = ConfNodeLookupChildValue(conf, "topic");
    if (unlikely(topic == NULL)) {
        SCLogError(SC_ERR_KAFKA_CONFIG, "kafka does not exist topic field in eve-log");
        return -1;
    }
    SCLogInfo("kafka topic: %s", topic);

    rd_kafka_topic_conf_t *topic_conf = NULL;

    ConfNode *topic_node = ConfNodeLookupChild(conf, "topic-configs");
    if (likely(topic_node != NULL)) {
        topic_conf = rd_kafka_topic_conf_new();
        if (unlikely(topic_conf == NULL)) {
            SCLogError(SC_ERR_KAFKA, "rd_kafka_topic_conf_new failed");
            return -1;
        }

        char err[KAFKA_ERR_MSG_LEN];
        ConfNode *node;
        TAILQ_FOREACH(node, &topic_node->head, next) {
            if (node->name == NULL || node->val == NULL) {
                continue;
            }
            SCLogConfig("kafka topic config: %s:%s", node->name, node->val);
            if (rd_kafka_topic_conf_set(topic_conf, node->name, node->val, err, KAFKA_ERR_MSG_LEN) !=
                    RD_KAFKA_CONF_OK) {
                SCLogError(SC_ERR_KAFKA_CONFIG, "%s:%s set failed: %s", node->name, node->val, err);
                continue;
            }
        }
    }
    /* Note: topic_conf object will be freed by rd_kafka_topic_new() function and must not be used
     * or destroyed by the application subsequently.
     */
    kafka->rkt = rd_kafka_topic_new(kafka->rk, topic, topic_conf);
    if (unlikely(kafka->rkt == NULL)) {
        SCLogError(SC_ERR_KAFKA, "rd_kafka_topic_new failed");
        return -1;
    }

    return 0;
}

static rd_kafka_conf_t *SCConfLogOpenKafkaProducer(ConfNode *conf, Kafka *kafka)
{
    const char *srv = ConfNodeLookupChildValue(conf, "bootstrap.servers");
    if (srv == NULL) {
        SCLogError(SC_ERR_KAFKA_CONFIG, "kafka does not exist bootstrap.servers field in eve-log");
        return NULL;
    }
    SCLogInfo("kafka bootstrap.servers: %s", srv);

    rd_kafka_conf_t *prod_conf = rd_kafka_conf_new();
    if (unlikely(prod_conf == NULL)) {
        SCLogError(SC_ERR_KAFKA, "rd_kafka_conf_new failed");
        return NULL;
    }

    char err[KAFKA_ERR_MSG_LEN];
    /* bootstrap.servers */
    if (rd_kafka_conf_set(prod_conf, "bootstrap.servers", srv, err, KAFKA_ERR_MSG_LEN) != 
            RD_KAFKA_CONF_OK) {
        SCLogError(SC_ERR_KAFKA_CONFIG, "bootstrap.servers:%s set failed: %s", srv, err);
        rd_kafka_conf_destroy(prod_conf);
        return NULL;
    }

    /* other producer config */
    ConfNode *prod_node = ConfNodeLookupChild(conf, "producer-configs");
    if (prod_node != NULL) {
        ConfNode *node;
        TAILQ_FOREACH(node, &prod_node->head, next) {
            if (node->name == NULL || node->val == NULL) {
                continue;
            }
            SCLogConfig("kafka producer config: %s:%s", node->name, node->val);
            if (rd_kafka_conf_set(prod_conf, node->name, node->val, err, KAFKA_ERR_MSG_LEN) !=
                    RD_KAFKA_CONF_OK) {
                SCLogError(SC_ERR_KAFKA_CONFIG, "%s:%s set failed: %s", node->name, node->val, err);
                continue;
            }
        }
    }

    return prod_conf;
}

int SCConfLogOpenKafka(ConfNode *conf, void *log_ctx)
{
    if (unlikely(conf == NULL || log_ctx == NULL)) {
        return -1;
    }

    LogFileCtx *file_ctx = log_ctx;
    char err[KAFKA_ERR_MSG_LEN];

    Kafka *kafka = (Kafka *)malloc(sizeof(Kafka));
    if (unlikely(kafka == NULL)) {
        SCLogError(SC_ERR_MEM_ALLOC, "kafka malloc failed");
        return -1;
    }
    memset(kafka, 0, sizeof(Kafka));
    /* producer configs */
    rd_kafka_conf_t *rkc = SCConfLogOpenKafkaProducer(conf, kafka);
    if (rkc == NULL) {
        free(kafka);
        return -1;
    }
    /* NOTE: The rkc object is freed by rd_kafka_new() function on success and must not be used
     * or destroyed by the application subsequently.
     */
    kafka->rk = rd_kafka_new(RD_KAFKA_PRODUCER, rkc, err, KAFKA_ERR_MSG_LEN);
    if (unlikely(kafka->rk == NULL)) {
        SCLogError(SC_ERR_KAFKA, "rd_kafka_new failed: %s", err);
        rd_kafka_conf_destroy(rkc);
        free(kafka);
        return -1;
    }
    /* topic configs */
    if (SCConfLogOpenKafkaTopic(conf, kafka) != 0) {
        rd_kafka_destroy(kafka->rk);
        free(kafka);
        return -1;
    }
    /* queue full retry config */
    int queue_full_retry = 0;
    ConfGetChildValueBool(conf, "queue-full-retry", &queue_full_retry);
    SCLogConfig("kafka queue-full-retry: %d", queue_full_retry);
    kafka->queue_full_retry = queue_full_retry;
    file_ctx->kafka = kafka;

    return 0;
}

int LogFileWriteKafka(void *log_ctx, void *buffer, size_t len)
{
    if (unlikely(log_ctx == NULL)) {
        return -1;
    }

    LogFileCtx *file_ctx = log_ctx;
    if (unlikely(file_ctx->kafka == NULL)) {
        return -1;
    }
    Kafka *kafka = file_ctx->kafka;

retry:
    if (rd_kafka_produce(kafka->rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, buffer, len, 
            NULL, 0, NULL) != 0) {
        rd_kafka_resp_err_t resp_err = rd_kafka_last_error();
        if (resp_err == RD_KAFKA_RESP_ERR__QUEUE_FULL && kafka->queue_full_retry) {
            SCLogWarning(SC_ERR_LOG_OUTPUT, 
                        "kafka producer message queue is full: %s, attempt to retry",
                        rd_kafka_err2str(resp_err));
            rd_kafka_poll(kafka->rk, 1000 /* block for max 1000ms */);
            goto retry;
        }
        SCLogError(SC_ERR_LOG_OUTPUT, "kafka producer resp error: %s", rd_kafka_err2str(resp_err));
        return -1;
    }
    rd_kafka_poll(kafka->rk, 0 /* non-blocking */);

    return 0;
}

void LogFileCloseKafka(void *log_ctx)
{
    if (unlikely(log_ctx == NULL)) {
        return;
    }

    LogFileCtx *file_ctx = log_ctx;
    if (unlikely(file_ctx->kafka == NULL)) {
        return;
    }

    Kafka *kafka = file_ctx->kafka;
    /* Wait for messages to be delivered */
    while (rd_kafka_outq_len(kafka->rk) > 0) {
        rd_kafka_poll(kafka->rk, 100 /* block for max 100ms */);
    }

    SCLogInfo("closing kafka");
    rd_kafka_topic_destroy(kafka->rkt);
    rd_kafka_destroy(kafka->rk);
    free(kafka);
}

#endif  /* HAVE_LIBRDKAFKA */
