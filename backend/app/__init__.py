"""slrno trading bot backend."""

from .fmp_proxy_fallbacks import install_fmp_proxy_fallbacks
from .payload_compaction import install_payload_compaction

install_fmp_proxy_fallbacks()
install_payload_compaction()
