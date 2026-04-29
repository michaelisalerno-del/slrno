from __future__ import annotations

from dataclasses import dataclass

from .providers.base import OHLCBar


@dataclass(frozen=True)
class TripleBarrierConfig:
    horizon_bars: int = 12
    profit_take_bps: float = 30.0
    stop_loss_bps: float = 20.0
    min_return_bps: float = 0.0


def forward_return_labels(bars: list[OHLCBar], horizon_bars: int = 12, min_return_bps: float = 0.0) -> list[int]:
    if horizon_bars <= 0:
        raise ValueError("horizon_bars must be positive")
    labels: list[int] = []
    threshold = min_return_bps / 10_000
    for index, bar in enumerate(bars):
        future_index = index + horizon_bars
        if future_index >= len(bars):
            labels.append(0)
            continue
        future_return = (bars[future_index].close - bar.close) / bar.close
        labels.append(1 if future_return > threshold else 0)
    return labels


def triple_barrier_labels(bars: list[OHLCBar], config: TripleBarrierConfig | None = None) -> list[int]:
    config = config or TripleBarrierConfig()
    if config.horizon_bars <= 0:
        raise ValueError("horizon_bars must be positive")
    labels: list[int] = []
    profit_take = config.profit_take_bps / 10_000
    stop_loss = config.stop_loss_bps / 10_000
    min_return = config.min_return_bps / 10_000

    for index, bar in enumerate(bars):
        end = min(len(bars), index + config.horizon_bars + 1)
        label = 0
        for future in bars[index + 1 : end]:
            high_return = (future.high - bar.close) / bar.close
            low_return = (future.low - bar.close) / bar.close
            if high_return >= profit_take:
                label = 1
                break
            if low_return <= -stop_loss:
                label = 0
                break
        else:
            if end - 1 > index:
                final_return = (bars[end - 1].close - bar.close) / bar.close
                label = 1 if final_return > min_return else 0
        labels.append(label)
    return labels
