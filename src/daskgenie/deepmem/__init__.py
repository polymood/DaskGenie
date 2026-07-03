"""Deep memory profiling: memray driven as a library, epoch-rotated, folded to
the user source line responsible for each high-water-mark allocation.
"""

from daskgenie.deepmem.tracker import DeepTracker

__all__ = ["DeepTracker"]
