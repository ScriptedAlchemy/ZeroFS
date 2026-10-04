FROM busybox:1.36
COPY mc /usr/local/bin/mc
ENV HOME=/tmp
ENTRYPOINT ["/usr/local/bin/mc"]
