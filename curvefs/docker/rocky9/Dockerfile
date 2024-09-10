FROM dingodatabase/dingofs-base:rocky9
COPY entrypoint.sh /
COPY curvefs /curvefs
COPY libetcdclient.so /usr/lib/
#COPY libmemcached.so libmemcached.so.11 libhashkit.so.2 /usr/lib/
RUN mkdir -p /etc/curvefs /core /etc/curve && chmod a+x /entrypoint.sh \
    && cp /curvefs/tools/sbin/curvefs_tool /usr/bin \
    && cp /curvefs/tools-v2/sbin/curve /usr/bin/ 
ENV LD_LIBRARY_PATH=/usr/lib:/usr/local/lib:$LD_LIBRARY_PATH
ENTRYPOINT ["/entrypoint.sh"]
