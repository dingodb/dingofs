FROM dingodatabase/dingo-base:rocky9

COPY entrypoint.sh /
COPY dingofs /dingofs
COPY dingofs/etcd/lib/libetcdclient.so /usr/lib/
RUN mkdir -p /etc/dingofs /core /etc/dingo && chmod a+x /entrypoint.sh \
    # && cp /dingofs/tools/sbin/dingo-tool /usr/bin \
    && cp /dingofs/tools-v2/sbin/dingo /usr/bin/  
ENV LD_LIBRARY_PATH=/usr/lib:/usr/lib64:/usr/local/lib:$LD_LIBRARY_PATH
ENTRYPOINT ["/entrypoint.sh"]
