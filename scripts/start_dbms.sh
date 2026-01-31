#!/bin/bash

current_dir=$(pwd)
dbms=$1
image_tag=$2
date=$(date --date="yesterday" '+%Y%m%d')

# check if image tag is set, else set it to 'latest'
if [ -z "$image_tag" ]; then
    image_tag="latest"
fi

# check if dbms is set
if [ -z "$dbms" ]; then
    echo "Please set dbms"
    exit 1
fi

# Run crate
if [ "$dbms" == "crate" ]; then
    docker stop crate-test
    docker rm crate-test
    docker run --name crate-test -p 10003:4200 -p 10004:5432 crate-source-build -Cdiscovery.type=single-node
fi
# docker build -t crate-source-build scripts/Docker/crate
# docker stop crate-test
# docker rm crate-test
# docker run --name crate-test -d -p 10003:4200 -p 10004:5432 crate-source-build  -Cdiscovery.type=single-node 

# Run dolt-server
if [ "$dbms" == "dolt" ]; then
    cd $current_dir/databases
    rm -rf dolt
    mkdir -p dolt
    cd dolt
    $HOME/go/bin/dolt sql-server -P 10007
fi
# docker stop dolt-test
# docker rm dolt-test
# docker run --name dolt-test -d -p 10007:3306 dolthub/dolt-sql-server:latest

# Run risingwave
if [ "$dbms" == "risingwave" ]; then
    docker stop risingwave-test
    docker rm risingwave-test
    docker run --name risingwave-test -p 10008:4566 risingwavelabs/risingwave:nightly-$date
fi
# docker stop risingwave-test
# docker rm risingwave-test
# docker run --name risingwave-test -d -p 10008:4566 risingwavelabs/risingwave:latest


# Run Firebird
if [ "$dbms" == "firebird" ]; then
    docker stop firebird-test
    docker rm firebird-test
    docker run --name firebird-test -p 10009:3050 firebird-source-build
fi
# reproduce: /usr/local/firebird/bin/isql
# docker stop firebird-test
# docker rm firebird-test
# # docker run --name firebird-test -e ISC_PASSWORD='masterkey' -e FIREBIRD_DATABASE='default' -d -p 10009:3050 jacobalberty/firebird
# docker build -t firebird-source-build scripts/Docker/firebird
# docker run --name firebird-test -d -p 10009:3050 firebird-source-build

# Run Postgres
if [ "$dbms" == "postgresql" ]; then
    docker stop postgresql-test
    docker rm postgresql-test
    docker run --name postgresql-test -e POSTGRES_PASSWORD=postgres -p 10010:5432 postgres:17rc1-bullseye
fi
# docker stop postgres-test
# docker rm postgres-test
# docker run --name postgres-test -e POSTGRES_PASSWORD=postgres -d -p 10010:5432 postgres

# Run CockroachDB
if [ "$dbms" == "cockroachdb" ]; then
    docker stop cockroachdb-test
    docker rm cockroachdb-test
    docker run --name cockroachdb-test -p 10011:26257 -p 10012:8080 cockroachdb/cockroach:latest start-single-node --insecure
fi
# docker stop cockroach-test
# docker rm cockroach-test
# docker run --name cockroach-test -d -p 10011:26257 -p 10012:8080 cockroachdb/cockroach:latest start-single-node --insecure

# Run TiDB
if [ "$dbms" == "tidb" ]; then
    docker stop tidb-test
    docker rm tidb-test
    docker run --name tidb-test -p 10013:4000 -p 10014:10080 pingcap/tidb:nightly
fi
# docker stop tidb-test
# docker rm tidb-test
# docker run --name tidb-test -d -p 10013:4000 -p 10014:10080 pingcap/tidb:nightly

# Run Umbra
if [ "$dbms" == "umbra" ]; then
    cd $current_dir/resources/umbra
    mkdir -p databases
    bin/sql --createdb databases/test $current_dir/scripts/configs/umbra/init.sql
    bin/server databases/test --port=10015 
fi

# docker stop umbra-test
# docker rm umbra-test
# docker run --name umbra-test -d -p 10015:5432 umbra-build:latest

# Run MariaDB
if [ "$dbms" == "mariadb" ]; then
    docker stop mariadb-test
    docker rm mariadb-test
    docker run --name mariadb-test -e MYSQL_ROOT_PASSWORD=root -p 10016:3306 mariadb:latest
fi
# docker stop mariadb-test
# docker rm mariadb-test
# docker run --name mariadb-test -e MYSQL_ROOT_PASSWORD=root -d -p 10016:3306 mariadb:latest

if [ "$dbms" == "mysql" ]; then
    docker stop mysql-test
    docker rm mysql-test
    docker run --name mysql-test -e MYSQL_ROOT_PASSWORD=root -p 23306:3306 mysql:latest
fi

if [ "$dbms" == "percona" ]; then
    docker stop percona-test
    docker rm percona-test
    docker run -d --name percona-test -p 10022:3306 -e MYSQL_ROOT_PASSWORD=root percona/percona-server:latest --character-set-server=utf8 --collation-server=utf8_general_ci
fi

if [ "$dbms" == "virtuoso" ]; then
    docker stop virtuoso-test
    docker rm virtuoso-test
    docker run --name virtuoso-test -p 10020:1111 -e DBA_PASSWORD=dba vos-reference:latest
fi

if [ "$dbms" == "monetdb" ]; then
    docker stop monetdb-test
    docker rm monetdb-test
    docker run --name monetdb-test -p 10021:50000 -e MDB_DB_ADMIN_PASS=monetdb monetdb-source-build
    # For debug:
    # docker exec monetdb-test bash -c 'echo -n -e "user=monetdb\npassword=monetdb\n" > /root/.monetdb'
    # docker exec -it monetdb-test mclient -E UTF-8 -d monetdb
    # For backtrace:
    # docker exec -it monetdb-test bash
    # monetdbd stop /var/monetdb5/dbfarm
    # gdb mserver5
fi

if [ "$dbms" == "h2" ]; then
    rm -rf $current_dir/databases/h2
    mkdir -p $current_dir/databases/h2
fi

if [ "$dbms" == "clickhouse" ]; then
    docker stop clickhouse-test
    docker rm clickhouse-test
    docker run --name clickhouse-test -p 10023:8123 -p 10024:9000 clickhouse/clickhouse-server:head-alpine

fi

if [ "$dbms" == "vitess" ]; then
    docker kill vitess-test
    docker rm vitess-test
    docker run --name vitess-test \
    -p 10025:33574 \
    -p 10026:33575 \
    -p 10027:33577 \
    -e PORT=33574 \
    -e KEYSPACES=test,unsharded \
    -e NUM_SHARDS=2,1 \
    -e MYSQL_MAX_CONNECTIONS=70000 \
    -e MYSQL_BIND_HOST=0.0.0.0 \
    -e VTCOMBO_BIND_HOST=0.0.0.0 \
    --health-cmd="mysqladmin ping -h127.0.0.1 -P33577" \
    --health-interval=5s \
    --health-timeout=2s \
    --health-retries=5 \
    vitess/vttestserver:mysql80
fi

if [ "$dbms" == "presto" ]; then
    docker stop presto-test
    docker rm presto-test
    docker run --name presto-test -p 10028:8080 -v $current_dir/scripts/configs/presto/memory.properties:/opt/presto-server/etc/catalog/memory.properties prestodb/presto:latest
fi

if [ "$dbms" == "opengauss" ]; then
    docker stop opengauss-test
    docker rm opengauss-test
    docker run --name opengauss-test -p 10032:5432 --privileged=true  -e GS_PASSWORD=openGauss@123 enmotech/opengauss:latest
fi

if [ "$dbms" == "oracle" ]; then
    docker stop oracle-test
    docker rm oracle-test
    docker run -p 10033:1521 --name oracle-test -e APP_USER=myuser -e APP_USER_PASSWORD=mypassword -e ORACLE_PASSWORD=oracle gvenzl/oracle-free
    # sqlplus myuser/mypassword@//localhost:1521/FREEPDB1
fi


if [ "$dbms" == "oceanbase" ]; then
    docker stop oceanbase-test
    docker rm oceanbase-test
    docker run -p 10031:2881 --name oceanbase-test -e MODE=mini -d oceanbase/oceanbase-ce:4.3.5.1-101000042025031818


fi

if [ "$dbms" == "materialize" ]; then
    docker stop materialize-test
    docker rm materialize-test
    docker run --name materialize-test -p 6875:6875 -p 6876:6876 materialize/materialized:latest
fi