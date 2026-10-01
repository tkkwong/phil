#!/usr/bin/env python3
"""Fixed Task Scheduler launcher for the disabled-by-default PAPER wrapper."""
from __future__ import annotations

import os
import pathlib


def main() -> int:
    repository_root = pathlib.Path(__file__).resolve().parent
    os.chdir(repository_root)
    from manus import scheduled_paper

    return scheduled_paper.main([])


if __name__ == "__main__":
    raise SystemExit(main())
