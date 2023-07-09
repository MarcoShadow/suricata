#ifndef __UTIL_LOG_KAFKA_H__
#define __UTIL_LOG_KAFKA_H__

#ifdef HAVE_LIBRDKAFKA
#include "librdkafka/rdkafka.h"

typedef struct Kafka_ {
    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    int queue_full_retry;
} Kafka;

int SCConfLogOpenKafka(ConfNode *, void *);
int LogFileWriteKafka(void *, void *, size_t);
void LogFileCloseKafka(void *);

#endif  /* HAVE_LIBRDKAFKA */
#endif  /* __UTIL_LOG_KAFKA_H__ */
