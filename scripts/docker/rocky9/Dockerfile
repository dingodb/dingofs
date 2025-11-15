FROM dingodatabase/dingo-base:rocky9

COPY dingofs /dingofs
RUN mkdir -p /etc/dingofs /core /etc/dingo \
    && cp /dingofs/tools/sbin/dingo /usr/bin/  
ENV LD_LIBRARY_PATH=/usr/lib:/usr/lib64:/usr/local/lib:$LD_LIBRARY_PATH
ENTRYPOINT ["/dingofs/scripts/clean_start.sh"]
