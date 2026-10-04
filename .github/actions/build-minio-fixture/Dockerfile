FROM scratch
COPY minio /minio
ENV HOME=/tmp USER=minio
EXPOSE 9000
ENTRYPOINT ["/minio"]
