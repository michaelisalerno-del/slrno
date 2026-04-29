from __future__ import annotations

from dataclasses import dataclass
from math import exp
from typing import Protocol

from .providers.base import OHLCBar


@dataclass(frozen=True)
class ProbabilityCandidate:
    name: str
    module_stack: tuple[str, ...]
    parameters: dict[str, float | int | str]
    probabilities: list[float]


class ProbabilityModule(Protocol):
    name: str

    def generate(self, bars: list[OHLCBar]) -> ProbabilityCandidate:
        ...


@dataclass(frozen=True)
class MomentumContinuation:
    window: int = 12
    name: str = "momentum_continuation"

    def generate(self, bars: list[OHLCBar]) -> ProbabilityCandidate:
        momentum = _rolling_return(bars, self.window)
        volatility = _rolling_abs_return(bars, self.window)
        probabilities = [
            _sigmoid((mom / vol) if vol > 0 else 0.0)
            for mom, vol in zip(momentum, volatility)
        ]
        return ProbabilityCandidate(self.name, (self.name, "volatility_scaled"), {"window": self.window}, probabilities)


@dataclass(frozen=True)
class MeanReversionStretch:
    window: int = 20
    name: str = "mean_reversion_stretch"

    def generate(self, bars: list[OHLCBar]) -> ProbabilityCandidate:
        zscores = _rolling_zscore([bar.close for bar in bars], self.window)
        probabilities = [_sigmoid(-zscore) for zscore in zscores]
        return ProbabilityCandidate(self.name, (self.name, "range_state"), {"window": self.window}, probabilities)


@dataclass(frozen=True)
class BreakoutContinuation:
    window: int = 24
    name: str = "breakout_continuation"

    def generate(self, bars: list[OHLCBar]) -> ProbabilityCandidate:
        probabilities: list[float] = []
        for index, bar in enumerate(bars):
            if index < self.window:
                probabilities.append(0.5)
                continue
            previous = bars[index - self.window : index]
            high = max(item.high for item in previous)
            low = min(item.low for item in previous)
            width = max(high - low, 1e-12)
            probabilities.append(_sigmoid((bar.close - high) / width * 4))
        return ProbabilityCandidate(self.name, (self.name, "compression_expansion"), {"window": self.window}, probabilities)


def default_probability_modules() -> list[ProbabilityModule]:
    return [
        MomentumContinuation(8),
        MomentumContinuation(16),
        MeanReversionStretch(12),
        MeanReversionStretch(24),
        BreakoutContinuation(16),
        BreakoutContinuation(32),
    ]


def _rolling_return(bars: list[OHLCBar], window: int) -> list[float]:
    values: list[float] = []
    for index, bar in enumerate(bars):
        if index < window:
            values.append(0.0)
        else:
            values.append((bar.close - bars[index - window].close) / bars[index - window].close)
    return values


def _rolling_abs_return(bars: list[OHLCBar], window: int) -> list[float]:
    values: list[float] = []
    for index in range(len(bars)):
        if index < window:
            values.append(0.0)
            continue
        returns = [
            abs((bars[item].close - bars[item - 1].close) / bars[item - 1].close)
            for item in range(index - window + 1, index + 1)
        ]
        values.append(sum(returns) / len(returns))
    return values


def _rolling_zscore(values: list[float], window: int) -> list[float]:
    zscores: list[float] = []
    for index, value in enumerate(values):
        if index < window:
            zscores.append(0.0)
            continue
        sample = values[index - window : index]
        mean = sum(sample) / len(sample)
        variance = sum((item - mean) ** 2 for item in sample) / len(sample)
        std = variance**0.5
        zscores.append(0.0 if std == 0 else (value - mean) / std)
    return zscores


def _sigmoid(value: float) -> float:
    value = max(-60.0, min(60.0, value))
    return 1 / (1 + exp(-value))
