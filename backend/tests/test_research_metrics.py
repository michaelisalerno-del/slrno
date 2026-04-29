from __future__ import annotations

from app.research_metrics import classification_metrics, precision_at_top_quantile, roc_auc


def test_roc_auc_scores_ranking_quality():
    labels = [0, 0, 1, 1]
    probabilities = [0.1, 0.4, 0.35, 0.9]

    assert roc_auc(labels, probabilities) == 0.75


def test_classification_metrics_include_auc_and_top_bucket_precision():
    metrics = classification_metrics([0, 1, 0, 1, 1], [0.1, 0.8, 0.2, 0.7, 0.9], top_quantile=0.4)

    assert metrics.roc_auc == 1.0
    assert metrics.pr_auc == 1.0
    assert metrics.precision_at_top_quantile == 1.0
    assert metrics.positive_rate == 0.6


def test_precision_at_top_quantile_validates_best_ranked_signals():
    assert precision_at_top_quantile([0, 1, 1, 0], [0.2, 0.9, 0.8, 0.1], 0.5) == 1.0
