"""Offset 0 replay utilities for Kafka consumer (REQ-C02, AC-C02)."""

from __future__ import annotations

from typing import Any, List
from snowflake_adapter.logging import get_logger

logger = get_logger(__name__)

try:
    from confluent_kafka import Consumer, TopicPartition
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False
    Consumer = None
    TopicPartition = Any


def rewind_topic_to_offset_zero(consumer: Any, topic: str) -> List[Any]:
    """
    Query metadata for topic, assign all partitions, and seek each partition to offset 0.
    Returns the list of assigned TopicPartition instances positioned at 0.
    """
    logger.info("Initiating offset 0 rewind for topic '%s'", topic)

    if hasattr(consumer, "list_topics"):
        metadata = consumer.list_topics(topic, timeout=10.0)
        topic_metadata = metadata.topics.get(topic)
        if not topic_metadata:
            raise ValueError(f"Topic '{topic}' not found in Kafka cluster metadata.")

        partitions_to_seek = [
            TopicPartition(topic, p_id, 0)
            for p_id in topic_metadata.partitions.keys()
        ]
        # Assign partitions directly to consumer
        consumer.assign(partitions_to_seek)

        # Explicitly seek each partition to offset 0
        for tp in partitions_to_seek:
            consumer.seek(tp)
            logger.info("Seeked %s [partition %d] to offset 0", topic, tp.partition)

        return partitions_to_seek

    elif hasattr(consumer, "seek_to_beginning"):
        # Mock consumer interface helper
        return consumer.seek_to_beginning(topic)

    else:
        logger.warning("Consumer does not support partition seeking.")
        return []
