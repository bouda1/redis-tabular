FROM redis:latest
MAINTAINER Centreon "support@centreon.com"

ENV LIBDIR /usr/lib/redis/modules
ADD build/redistabular.so "$LIBDIR/redistabular.so"

CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redistabular.so"]
