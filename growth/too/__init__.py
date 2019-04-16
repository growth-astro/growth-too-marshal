import redis.selector

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

# Set up Redis to use poll() instead of select() because Python's
# poll() function is not thread safe and therefore does not calling.get()
# on multiple Celery results concurrently from different threads.
#
# See https://bugs.python.org/issue8865
redis.selector._DEFAULT_SELECTOR = redis.selector.SelectSelector
