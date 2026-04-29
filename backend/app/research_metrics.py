from __future__ import annotations

from dataclasses import dataclass
from math import log


@dataclass(frozen=True)
class ClassificationMetrics:
    roc_auc: float | None
    pr_auc: float | None
    brier_score: float
    log_loss: float
    precision_at_top_quantile: float
    positive_rate: float
    sample_count: int


def classification_metrics(labels: list[int], probabilities: list[float], top_quantile: float = 0.2) -> ClassificationMetrics:
    if len(labels) != len(probabilities):
        raise ValueError("labels and probabilities must have the same length")
    if not labels:
        raise ValueError("at least one label is required")
    if not 0 < top_quantile <= 1:
        raise ValueError("top_quantile must be between 0 and 1")

    clean_labels = [1 if label > 0 else 0 for label in labels]
    clean_probabilities = [_clip_probability(value) for value in probabilities]
    positives = sum(clean_labels)
    sample_count = len(clean_labels)
    positive_rate = positives / sample_count

    return ClassificationMetrics(
        roc_auc=roc_auc(clean_labels, clean_probabilities),
        pr_auc=pr_auc(clean_labels, clean_probabilities),
        brier_score=sum((prob - label) ** 2 for label, prob in zip(clean_labels, clean_probabilities)) / sample_count,
        log_loss=-sum(
            label * log(prob) + (1 - label) * log(1 - prob)
            for label, prob in zip(clean_labels, clean_probabilities)
        )
        / sample_count,
        precision_at_top_quantile=precision_at_top_quantile(clean_labels, clean_probabilities, top_quantile),
        positive_rate=positive_rate,
        sample_count=sample_count,
    )


def roc_auc(labels: list[int], probabilities: list[float]) -> float | None:
    positives = sum(labels)
    negatives = len(labels) - positives
    if positives == 0 or negatives == 0:
        return None

    ranked = sorted(enumerate(probabilities), key=lambda item: item[1])
    ranks = [0.0] * len(probabilities)
    index = 0
    while index < len(ranked):
        end = index + 1
        while end < len(ranked) and ranked[end][1] == ranked[index][1]:
            end += 1
        average_rank = (index + 1 + end) / 2
        for rank_index in range(index, end):
            ranks[ranked[rank_index][0]] = average_rank
        index = end

    positive_rank_sum = sum(rank for label, rank in zip(labels, ranks) if label == 1)
    return (positive_rank_sum - positives * (positives + 1) / 2) / (positives * negatives)


def pr_auc(labels: list[int], probabilities: list[float]) -> float | None:
    positives = sum(labels)
    if positives == 0:
        return None

    pairs = sorted(zip(probabilities, labels), key=lambda item: item[0], reverse=True)
    true_positives = 0
    false_positives = 0
    points: list[tuple[float, float]] = [(0.0, 1.0)]
    for _, label in pairs:
        if label == 1:
            true_positives += 1
        else:
            false_positives += 1
        recall = true_positives / positives
        precision = true_positives / (true_positives + false_positives)
        points.append((recall, precision))

    area = 0.0
    previous_recall = 0.0
    for recall, precision in points[1:]:
        area += (recall - previous_recall) * precision
        previous_recall = recall
    return area


def precision_at_top_quantile(labels: list[int], probabilities: list[float], top_quantile: float) -> float:
    take = max(1, int(round(len(labels) * top_quantile)))
    pairs = sorted(zip(probabilities, labels), key=lambda item: item[0], reverse=True)
    selected = pairs[:take]
    return sum(label for _, label in selected) / len(selected)


def _clip_probability(value: float) -> float:
    return min(1 - 1e-12, max(1e-12, float(value)))
