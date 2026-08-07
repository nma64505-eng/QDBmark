FROM docker.m.daocloud.io/library/centos:7.9.2009 AS ycsb-builder

WORKDIR /tmp

ENV YCSB_VERSION=0.17.0

COPY tools/build-scripts/patch_ycsb_mongodb.py /tmp/patch_ycsb_mongodb.py

RUN set -eux; \
    printf '%s\n' \
      '[base]' \
      'name=CentOS-7.9.2009 - Base' \
      'baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/os/x86_64/' \
      'gpgcheck=0' \
      'enabled=1' \
      '[updates]' \
      'name=CentOS-7.9.2009 - Updates' \
      'baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/updates/x86_64/' \
      'gpgcheck=0' \
      'enabled=1' \
      '[extras]' \
      'name=CentOS-7.9.2009 - Extras' \
      'baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/extras/x86_64/' \
      'gpgcheck=0' \
      'enabled=1' \
      '[epel]' \
      'name=EPEL 7' \
      'baseurl=https://mirrors.aliyun.com/epel/7/x86_64/' \
      'gpgcheck=0' \
      'enabled=1' \
      >/etc/yum.repos.d/CentOS-Base.repo; \
    printf '%s\n' '[main]' 'enabled=0' >/etc/yum/pluginconf.d/fastestmirror.conf; \
    yum clean all; \
    yum install -y git maven java-1.8.0-openjdk-devel python3 ca-certificates tar; \
    yum clean all; \
    rm -rf /var/cache/yum

RUN set -eux; \
    for attempt in 1 2 3 4 5; do \
      rm -rf /tmp/YCSB; \
      git clone --depth 1 --branch "${YCSB_VERSION}" https://github.com/brianfrankcooper/YCSB /tmp/YCSB && break; \
      sleep $((attempt * 5)); \
    done; \
    test -d /tmp/YCSB/.git \
    && cd /tmp/YCSB \
    && python3 /tmp/patch_ycsb_mongodb.py /tmp/YCSB \
    && mvn -pl site.ycsb:mongodb-binding -am -DskipTests -Dcheckstyle.skip=true -Denforcer.skip=true clean package

FROM docker.m.daocloud.io/library/centos:7.9.2009

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/backend
ENV PORT=12365
ENV YCSB_VERSION=0.17.0
ENV YCSB_HOME=/app/tools/ycsb-mongodb
ENV KAFKA_VERSION=3.7.2
ENV KAFKA_SCALA_VERSION=2.13
ENV KAFKA_HOME=/app/tools/kafka
ENV ROCKETMQ_VERSION=5.5.0
ENV ROCKETMQ_HOME=/app/tools/rocketmq
ENV RABBITMQ_PERF_TEST_VERSION=2.24.0
ENV RABBITMQ_PERF_TEST_JAR=/app/tools/rabbitmq-perf-test/perf-test.jar
ENV MYSQL_PLUGIN_DIR=/usr/lib64/mysql/plugin
ENV LIBMYSQL_PLUGIN_DIR=/usr/lib64/mysql/plugin
ENV MARIADB_PLUGIN_DIR=/usr/lib64/mysql/plugin
ENV OCEANBASE_BENCHMARKSQL_HOME=/app/tools/benchmarksql-oceanbase
ENV KINGBASE_BENCHMARKSQL_HOME=/app/tools/benchmarksql-kingbase
ENV HAMMERDB_HOME=/opt/HammerDB-4.0
ENV HAMMERDB_CLI=/opt/HammerDB-4.0/hammerdbcli
ENV SWINGBENCH_HOME=/opt/swingbench
ENV ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient:/opt/oracle/instantclient/lib:/usr/lib64:/usr/lib
ENV PATH=/app/tools/rocketmq/bin:/app/tools/kafka/bin:/opt/swingbench/bin:/opt/conda/bin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

COPY tools/hammerdb-oracle-sqlserver-cache /tmp/hammerdb-cache
COPY tools/instantclient-oracle-cache /tmp/oracle-cache
COPY tools/swingbench-oracle-cache /tmp/swingbench-cache
COPY tools/benchmarksql-dm-cache /tmp/benchmarksql-dm-cache
COPY requirements.txt .

RUN set -eux; \
    printf '%s\n' \
      '[base]' \
      'name=CentOS-7.9.2009 - Base' \
      'baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/os/x86_64/' \
      'gpgcheck=0' \
      'enabled=1' \
      '[updates]' \
      'name=CentOS-7.9.2009 - Updates' \
      'baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/updates/x86_64/' \
      'gpgcheck=0' \
      'enabled=1' \
      '[extras]' \
      'name=CentOS-7.9.2009 - Extras' \
      'baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/extras/x86_64/' \
      'gpgcheck=0' \
      'enabled=1' \
      '[epel]' \
      'name=EPEL 7' \
      'baseurl=https://mirrors.aliyun.com/epel/7/x86_64/' \
      'gpgcheck=0' \
      'enabled=1' \
      >/etc/yum.repos.d/CentOS-Base.repo; \
    printf '%s\n' \
      '[packages-microsoft-com-prod]' \
      'name=packages-microsoft-com-prod' \
      'baseurl=https://packages.microsoft.com/rhel/7/prod/' \
      'enabled=1' \
      'gpgcheck=0' \
      >/etc/yum.repos.d/msprod.repo; \
    printf '%s\n' \
      '[mariadb]' \
      'name=MariaDB 10.11' \
      'baseurl=https://rpm.mariadb.org/10.11/centos7-amd64' \
      'enabled=1' \
      'gpgcheck=0' \
      >/etc/yum.repos.d/mariadb.repo; \
    printf '%s\n' '[main]' 'enabled=0' >/etc/yum/pluginconf.d/fastestmirror.conf; \
    yum clean all; \
    ACCEPT_EULA=Y yum install -y \
      sysbench MariaDB-common MariaDB-compat fio redis libreoffice-writer wqy-microhei-fonts openssh-clients sshpass \
      java-1.8.0-openjdk-devel java-11-openjdk-devel ant ca-certificates tar postgresql postgresql-contrib \
      curl unzip unixODBC libiodbc libaio findutils which msodbcsql17; \
    yum clean all; \
    test -f /usr/lib64/mysql/plugin/caching_sha2_password.so; \
    rm -rf /var/cache/yum; \
    curl -fsSL https://repo.anaconda.com/miniconda/Miniconda3-py310_24.7.1-0-Linux-x86_64.sh -o /tmp/miniconda.sh; \
    bash /tmp/miniconda.sh -b -p /opt/conda; \
    rm -f /tmp/miniconda.sh; \
    /opt/conda/bin/python -m pip install --upgrade 'pip<26'; \
    if [ -f /tmp/hammerdb-cache/HammerDB-4.0-Linux.tar.gz ]; then \
      cp /tmp/hammerdb-cache/HammerDB-4.0-Linux.tar.gz /tmp/hammerdb.tar.gz; \
    else \
      export HAMMERDB_URL='https://github.com/TPC-Council/HammerDB/releases/download/v4.0/HammerDB-4.0-Linux.tar.gz'; \
      for attempt in 1 2 3 4 5; do \
        rm -f /tmp/hammerdb.tar.gz; \
        curl -fL --connect-timeout 30 --retry 3 --retry-delay 5 "$HAMMERDB_URL" -o /tmp/hammerdb.tar.gz && break; \
        sleep $((attempt * 5)); \
      done; \
    fi; \
    tar -xzf /tmp/hammerdb.tar.gz -C /opt; \
    chmod 755 /opt/HammerDB-4.0/hammerdbcli; \
    ln -sf /opt/HammerDB-4.0/hammerdbcli /usr/local/bin/hammerdbcli; \
    test -x /usr/local/bin/hammerdbcli; \
    if [ -f /tmp/swingbench-cache/swingbench04112023_jdk11.zip ]; then \
      cp /tmp/swingbench-cache/swingbench04112023_jdk11.zip /tmp/swingbench.zip; \
    else \
      export SWINGBENCH_URL='https://downloads.dominicgiles.com/swingbench04112023_jdk11.zip'; \
      for attempt in 1 2 3 4 5; do \
        rm -f /tmp/swingbench.zip; \
        curl -fL --connect-timeout 30 --retry 3 --retry-delay 5 "$SWINGBENCH_URL" -o /tmp/swingbench.zip && break; \
        sleep $((attempt * 5)); \
      done; \
    fi; \
    rm -rf /opt/swingbench /tmp/swingbench-extract; \
    mkdir -p /tmp/swingbench-extract; \
    unzip -q /tmp/swingbench.zip -d /tmp/swingbench-extract; \
    if [ -d /tmp/swingbench-extract/swingbench ]; then \
      mv /tmp/swingbench-extract/swingbench /opt/swingbench; \
    else \
      SWINGBENCH_SRC="$(find /tmp/swingbench-extract -maxdepth 2 -type f -path '*/bin/charbench' | head -n 1 | xargs -r dirname | xargs -r dirname)"; \
      test -n "$SWINGBENCH_SRC"; \
      mv "$SWINGBENCH_SRC" /opt/swingbench; \
    fi; \
    chmod +x /opt/swingbench/bin/*; \
    JAVA11_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-11-openjdk*' | head -n 1)"; \
    test -n "$JAVA11_HOME"; \
    ln -sf "$JAVA11_HOME/bin/java" /usr/local/bin/java; \
    /usr/local/bin/java -version; \
    test -x /opt/swingbench/bin/charbench; \
    test -x /opt/swingbench/bin/oewizard; \
    rm -rf /tmp/swingbench.zip /tmp/swingbench-extract; \
    if [ -f /tmp/oracle-cache/instantclient-basiclite-linux.x64-19.30.0.0.0dbru.zip ]; then \
      cp /tmp/oracle-cache/instantclient-basiclite-linux.x64-19.30.0.0.0dbru.zip /tmp/oracle-instantclient.zip; \
    else \
      export ORACLE_CLIENT_URL='https://download.oracle.com/otn_software/linux/instantclient/1930000/instantclient-basiclite-linux.x64-19.30.0.0.0dbru.zip'; \
      for attempt in 1 2 3 4 5; do \
        rm -f /tmp/oracle-instantclient.zip; \
        curl -fL --connect-timeout 30 --retry 3 --retry-delay 5 "$ORACLE_CLIENT_URL" -o /tmp/oracle-instantclient.zip && break; \
        sleep $((attempt * 5)); \
      done; \
    fi; \
    mkdir -p /opt/oracle; \
    unzip -q /tmp/oracle-instantclient.zip -d /opt/oracle; \
    ORACLE_CLIENT_DIR=$(find /opt/oracle -maxdepth 1 -type d -name 'instantclient_*' | head -n 1); \
    test -n "$ORACLE_CLIENT_DIR"; \
    ln -sfn "$ORACLE_CLIENT_DIR" /opt/oracle/instantclient; \
    mkdir -p /opt/oracle/instantclient/lib; \
    for f in /opt/oracle/instantclient/*.so*; do ln -sfn "$f" "/opt/oracle/instantclient/lib/$(basename "$f")"; done; \
    echo /opt/oracle/instantclient >/etc/ld.so.conf.d/oracle-instantclient.conf; \
    echo /opt/oracle/instantclient/lib >/etc/ld.so.conf.d/oracle-instantclient-lib.conf; \
    /sbin/ldconfig; \
    test -n "$(find -L /opt/oracle/instantclient -maxdepth 1 -name 'libclntsh.so*' -print -quit)"; \
    test -n "$(find -L /opt/oracle/instantclient/lib -maxdepth 1 -name 'libclntsh.so*' -print -quit)"; \
    /opt/conda/bin/pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple --default-timeout=120 -r requirements.txt; \
    /opt/conda/bin/python -m venv /opt/esrally-venv; \
    /opt/esrally-venv/bin/pip install --upgrade 'pip<26'; \
    for attempt in 1 2 3; do \
      /opt/esrally-venv/bin/pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple --default-timeout=300 --retries=10 esrally==2.13.0 && break; \
      sleep $((attempt * 10)); \
    done

ENV TIUP_HOME=/root/.tiup
ENV PATH=/root/.tiup/bin:/opt/conda/bin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN set -eux; \
    curl -fsSL https://tiup-mirrors.pingcap.com/install.sh -o /tmp/install_tiup.sh; \
    bash /tmp/install_tiup.sh; \
    rm -f /tmp/install_tiup.sh; \
    ln -sf /root/.tiup/bin/tiup /usr/local/bin/tiup; \
    tiup install bench; \
    tiup bench tpcc --help >/tmp/tiup-bench-tpcc-help.txt

COPY . .
COPY --from=ycsb-builder /tmp/YCSB/mongodb/target/ycsb-mongodb-binding-0.17.0.tar.gz /tmp/ycsb-mongodb-binding.tar.gz

RUN set -eux; \
    ln -sfn /app/tools /app/vendor; \
    rm -rf /app/tools/kafka /tmp/kafka.tgz; \
    KAFKA_DOWNLOAD_VERSION="3.9.2"; \
    KAFKA_TGZ="kafka_${KAFKA_SCALA_VERSION}-${KAFKA_DOWNLOAD_VERSION}.tgz"; \
    for base_url in \
      "https://mirrors.aliyun.com/apache/kafka/${KAFKA_DOWNLOAD_VERSION}" \
      "https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/${KAFKA_DOWNLOAD_VERSION}" \
      "https://downloads.apache.org/kafka/${KAFKA_DOWNLOAD_VERSION}" \
      "https://archive.apache.org/dist/kafka/${KAFKA_DOWNLOAD_VERSION}"; do \
      for attempt in 1 2 3; do \
        rm -f /tmp/kafka.tgz; \
        curl -fL --connect-timeout 20 --speed-time 20 --speed-limit 102400 --retry 2 --retry-delay 3 "${base_url}/${KAFKA_TGZ}" -o /tmp/kafka.tgz && break 2; \
        sleep $((attempt * 3)); \
      done; \
    done; \
    test -s /tmp/kafka.tgz; \
    tar -xzf /tmp/kafka.tgz -C /app/tools; \
    mv "/app/tools/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_DOWNLOAD_VERSION}" /app/tools/kafka; \
    chmod +x /app/tools/kafka/bin/*.sh; \
    test -x /app/tools/kafka/bin/kafka-producer-perf-test.sh; \
    test -x /app/tools/kafka/bin/kafka-consumer-perf-test.sh; \
    rm -f /tmp/kafka.tgz; \
    rm -rf /app/tools/rocketmq /tmp/rocketmq.zip; \
    ROCKETMQ_DOWNLOAD_VERSION="${ROCKETMQ_VERSION:-5.5.0}"; \
    ROCKETMQ_ZIP="rocketmq-all-${ROCKETMQ_DOWNLOAD_VERSION}-bin-release.zip"; \
    for base_url in \
      "https://mirrors.aliyun.com/apache/rocketmq/${ROCKETMQ_DOWNLOAD_VERSION}" \
      "https://mirrors.tuna.tsinghua.edu.cn/apache/rocketmq/${ROCKETMQ_DOWNLOAD_VERSION}" \
      "https://downloads.apache.org/rocketmq/${ROCKETMQ_DOWNLOAD_VERSION}" \
      "https://archive.apache.org/dist/rocketmq/${ROCKETMQ_DOWNLOAD_VERSION}"; do \
      for attempt in 1 2 3; do \
        rm -f /tmp/rocketmq.zip; \
        curl -fL --connect-timeout 20 --speed-time 20 --speed-limit 102400 --retry 2 --retry-delay 3 "${base_url}/${ROCKETMQ_ZIP}" -o /tmp/rocketmq.zip && break 2; \
        sleep $((attempt * 3)); \
      done; \
    done; \
    test -s /tmp/rocketmq.zip; \
    unzip -q /tmp/rocketmq.zip -d /app/tools; \
    mv "/app/tools/rocketmq-all-${ROCKETMQ_DOWNLOAD_VERSION}-bin-release" /app/tools/rocketmq; \
    chmod +x /app/tools/rocketmq/bin/*.sh /app/tools/rocketmq/bin/mqadmin || true; \
    test -x /app/tools/rocketmq/bin/tools.sh; \
    test -x /app/tools/rocketmq/bin/mqadmin; \
    rm -f /tmp/rocketmq.zip; \
    if [ -s /app/tools/rabbitmq-perf-test/perf-test.jar ]; then \
      chmod 644 /app/tools/rabbitmq-perf-test/perf-test.jar; \
    else \
      rm -rf /app/tools/rabbitmq-perf-test /tmp/rabbitmq-perf-test.jar; \
      mkdir -p /app/tools/rabbitmq-perf-test; \
      export RABBITMQ_PERF_TEST_URL="https://github.com/rabbitmq/rabbitmq-perf-test/releases/download/v${RABBITMQ_PERF_TEST_VERSION}/perf-test-${RABBITMQ_PERF_TEST_VERSION}.jar"; \
      for attempt in 1 2 3 4 5; do \
        rm -f /tmp/rabbitmq-perf-test.jar; \
        curl -fL --connect-timeout 30 --speed-time 30 --speed-limit 20480 --retry 3 --retry-delay 5 "$RABBITMQ_PERF_TEST_URL" -o /tmp/rabbitmq-perf-test.jar && break; \
        sleep $((attempt * 5)); \
      done; \
      test -s /tmp/rabbitmq-perf-test.jar; \
      cp /tmp/rabbitmq-perf-test.jar /app/tools/rabbitmq-perf-test/perf-test.jar; \
      rm -f /tmp/rabbitmq-perf-test.jar; \
    fi; \
    test -s /app/tools/rabbitmq-perf-test/perf-test.jar; \
    mkdir -p /app/tools/sysbench-mysql-postgresql-vastbase/bin /app/tools/sysbench-mysql-postgresql-vastbase/share; \
    cp /usr/bin/sysbench /app/tools/sysbench-mysql-postgresql-vastbase/bin/sysbench; \
    cp -R /usr/share/sysbench /app/tools/sysbench-mysql-postgresql-vastbase/share/sysbench; \
    ln -sfn /app/tools/sysbench-mysql-postgresql-vastbase /app/tools/sysbench; \
    if [ -f /tmp/benchmarksql-dm-cache/benchmarksql-dm-x86.tar ]; then \
      tar -xf /tmp/benchmarksql-dm-cache/benchmarksql-dm-x86.tar -C /app/tools; \
      if [ -d /app/tools/benchmarksqlforDM ]; then mv /app/tools/benchmarksqlforDM /app/tools/benchmarksql-dm; fi; \
      ln -sfn /app/tools/benchmarksql-dm /app/tools/benchmarksqlforDM; \
      chmod +x /app/tools/benchmarksql-dm/tpcc_load.sh /app/tools/benchmarksql-dm/tpcc_test.sh /app/tools/benchmarksql-dm/bin/* || true; \
    else \
      echo 'benchmarksql-dm-x86.tar not found, skip DM BenchmarkSQL runtime installation'; \
    fi; \
    tar -xzf /tmp/ycsb-mongodb-binding.tar.gz -C /app/tools; \
    rm -rf /app/tools/ycsb-mongodb; \
    mv "/app/tools/ycsb-mongodb-binding-${YCSB_VERSION}" /app/tools/ycsb-mongodb; \
    ln -sfn /app/tools/ycsb-mongodb /app/tools/ycsb; \
    rm -f /tmp/ycsb-mongodb-binding.tar.gz; \
    rm -rf /app/tools/benchmarksql-oceanbase /tmp/benchmarksql-oceanbase-extract; \
    if [ -d /app/tools/benchmarksql-oceanbase-src/benchmarksql-mysql ]; then \
      cp -a /app/tools/benchmarksql-oceanbase-src/benchmarksql-mysql /app/tools/benchmarksql-oceanbase; \
    elif [ -f /app/tools/benchmarksql-oceanbase-src.tar.gz ]; then \
      mkdir -p /tmp/benchmarksql-oceanbase-extract; \
      tar -xzf /app/tools/benchmarksql-oceanbase-src.tar.gz -C /tmp/benchmarksql-oceanbase-extract; \
      cp -a /tmp/benchmarksql-oceanbase-extract/benchmarksql5.0-oceanbase/benchmarksql-mysql /app/tools/benchmarksql-oceanbase; \
    elif [ -f /app/tools/benchmarksql-oceanbase-master.tar.gz ]; then \
      mkdir -p /app/tools/benchmarksql-oceanbase; \
      tar -xzf /app/tools/benchmarksql-oceanbase-master.tar.gz -C /app/tools/benchmarksql-oceanbase --strip-components=1; \
    else \
      echo 'missing OceanBase BenchmarkSQL package: tools/benchmarksql-oceanbase-src.tar.gz' >&2; \
      exit 1; \
    fi; \
    chmod +x /app/tools/benchmarksql-oceanbase/run/*.sh; \
    test -f /app/tools/benchmarksql-oceanbase/run/props.oceanbase; \
    test -f /app/tools/benchmarksql-oceanbase/dist/BenchmarkSQL-5.0.jar; \
    rm -rf /tmp/benchmarksql-oceanbase-extract; \
    KINGBASE_ARCHIVE="$(find /app/tools -maxdepth 1 -type f \( -name 'benchmarksql-5.0-kingbase.tar' -o -name 'benchmarksql-5.0-kingbase.tar.gz' -o -name 'benchmarksql-kingbase.tar' -o -name 'benchmarksql-kingbase.tar.gz' -o -name 'benchmarksql-kingbase.zip' \) | head -n 1)"; \
    if [ -n "$KINGBASE_ARCHIVE" ]; then \
      rm -rf /app/tools/benchmarksql-kingbase; \
      mkdir -p /app/tools/benchmarksql-kingbase; \
      case "$KINGBASE_ARCHIVE" in \
        *.zip) unzip -q "$KINGBASE_ARCHIVE" -d /tmp/benchmarksql-kingbase-extract ;; \
        *.tar|*.tar.gz) mkdir -p /tmp/benchmarksql-kingbase-extract; tar -xf "$KINGBASE_ARCHIVE" -C /tmp/benchmarksql-kingbase-extract ;; \
      esac; \
      KINGBASE_SRC="$(find /tmp/benchmarksql-kingbase-extract -maxdepth 2 -type f -path '*/run/runBenchmark.sh' | head -n 1 | xargs -r dirname | xargs -r dirname)"; \
      if [ -n "$KINGBASE_SRC" ]; then cp -a "$KINGBASE_SRC"/. /app/tools/benchmarksql-kingbase/; fi; \
      chmod +x /app/tools/benchmarksql-kingbase/run/*.sh || true; \
      rm -rf /tmp/benchmarksql-kingbase-extract; \
    fi; \
    if [ -d /app/tools/benchmarksql-kingbase ]; then \
      chmod +x /app/tools/benchmarksql-kingbase/run/*.sh || true; \
      if [ -f /app/tools/benchmarksql-kingbase/build.xml ]; then \
        cd /app/tools/benchmarksql-kingbase; \
        ant clean dist; \
        cd /app; \
      fi; \
      test -f /app/tools/benchmarksql-kingbase/dist/BenchmarkSQL-5.0.jar; \
      test -n "$(find /app/tools/benchmarksql-kingbase/lib/kingbase -maxdepth 1 -type f -name '*kingbase*.jar' -print -quit)"; \
    fi

ENV KAFKA_VERSION=3.9.2

EXPOSE 12365

CMD ["waitress-serve", "--host=0.0.0.0", "--port=12365", "app:app"]
