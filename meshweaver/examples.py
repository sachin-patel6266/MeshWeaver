"""Small functions used by the documentation and local demonstration."""

from __future__ import annotations

import math


def count_primes(limit: int) -> dict[str, int]:
    """Count primes below ``limit`` using trial division."""
    if limit < 2:
        return {"limit": limit, "count": 0}
    count = 0
    for candidate in range(2, limit):
        is_prime = True
        for divisor in range(2, math.isqrt(candidate) + 1):
            if candidate % divisor == 0:
                is_prime = False
                break
        count += is_prime
    return {"limit": limit, "count": count}


def sum_squares(values: list[int]) -> int:
    """Return the sum of squares for a list of integers."""
    return sum(value * value for value in values)

