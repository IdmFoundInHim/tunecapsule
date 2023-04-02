""" TuneCapsule for StreamSort

Copyright (c) 2021 IdmFoundInHim, under MIT License
"""
# pyright: reportUnsupportedDunderAll=false
from .sentences import __all__ as sentences__all__
from .stats import __all__ as stats__all__

__all__ = ["_sentences_"] + sentences__all__ + stats__all__

from .sentences import *

_sentences_ = {"classify": ss_classify, "season": ss_season, "score": ss_score}
