#/bin/sh
set -e

SCALE=1
OUTPUT_BASE="."
RANDSEED=1530800000
DFLTSUB=false
VERBOSE=false
LOAD=false

while getopts ":s:o:r:dvL" opt; do
    case ${opt} in
        s )
            SCALE=$OPTARG
            ;;
        o )
            OUTPUT_BASE=$OPTARG
            ;;
        r )
            RANDSEED=$OPTARG
            ;;
        d )
            DFLTSUB=true
            ;;
        v )
            VERBOSE=true
            ;;
        L )
            LOAD=true
            ;;
        \? )
            echo "Invalid option: $OPTARG" 1>&2
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
QNUM=${@:$OPTIND:1}
shift $((OPTIND -1))

MAPDQL_PASSWD=HyperInteractive
DBGEN_HOME=2.17.3/dbgen
LOG_DIR=$OUTPUT_BASE/mapd_log_$(date +%Y%m%d%H%M%S)
export DSS_PATH=${LOG_DIR}
export DSS_CONFIG=${DBGEN_HOME}
export DSS_QUERY=${DBGEN_HOME}/queries

if [ ! -d ${LOG_DIR} ]; then
    mkdir -p ${LOG_DIR}
fi

function getExecTime() {
    start=$1
    end=$2
    time_s=`echo "scale=3;$(($end-$start))/1000" | bc`
    echo "Duration: ${time_s} s"
}

function Load() {

    echo "Dropping tables."
    for t in LINEITEM CUSTOMER NATION ORDERS PART PARTSUPP REGION SUPPLIER
    do
        echo "DROP TABLE IF EXISTS ${t};" | $MAPD_PATH/bin/mapdql -p $MAPDQL_PASSWD -q 2>&1 | tee -a ${LOG_DIR}/load.log
    done

    echo "Creating tables."
    tail -n +2 $DBGEN_HOME/dss.ddl | $MAPD_PATH/bin/mapdql -p $MAPDQL_PASSWD -q 2>&1 | tee -a ${LOG_DIR}/load.log

    echo "Generating text data into files."
    FREE_MEM=$(echo "$(awk '/MemFree/{print$2}' /proc/meminfo)/1024/1024" | bc)
    CPU_UTL=$(awk '/cpu /{printf("%.4f", ($2+$4)/($2+$4+$5))}' /proc/stat)
    CPU_CNT=$(awk '/^processor\t/{print$3}' /proc/cpuinfo | wc -l)
    CPU_USE=$(echo "(1-$CPU_UTL)*$CPU_CNT" | bc | awk -F. '{print$1}')
    #chunks=$(echo "($SCALE/$FREE_MEM+1)*$(echo $(($CPU_USE<4?$CPU_USE:4)))" | bc)
    chunks=$(echo "($SCALE/$FREE_MEM+1)*$CPU_USE" | bc)
    if [ $chunks -lt 1 ]; then
        echo "System too busy to generate data." 1>&2
        exit 1
    fi
    var=""
    for t in L c O P S s
    do
        for x in $(seq 1 $chunks)
        do
            var+="${DBGEN_HOME} ${SCALE} ${t} $chunks $x "
        done
    done
    if [ "$VERBOSE" = true ]
    then
        echo $var | xargs -n 5 -P ${CPU_USE} sh -c '${1}/dbgen -s ${2} -T ${3}  -C $4 -S $5 -v' sh
        $DBGEN_HOME/dbgen -s ${SCALE} -T l -v
    else
        echo $var | xargs -n 5 -P ${CPU_USE} sh -c '${1}/dbgen -s ${2} -T ${3}  -C $4 -S $5' sh
        $DBGEN_HOME/dbgen -s ${SCALE} -T l
    fi

    echo "Loading text data into external tables."
    for t in ${DSS_PATH}/*.tbl.*
    #for t in LINEITEM CUSTOMER NATION ORDERS PART PARTSUPP REGION SUPPLIER
    do
        tbl_name=$(echo $(basename ${t}) | awk '{split($0,a,"."); print toupper(a[1])}')
        sed -i 's/|$//' $t
        echo "Start loading ${t} ..." 2>&1 | tee -a ${LOG_DIR}/load.log
        date 2>&1 | tee -a ${LOG_DIR}/load.log
        echo "COPY ${tbl_name} FROM '${t}' WITH (delimiter='|', header='false', quoted='false');" | $MAPD_PATH/bin/mapdql -p $MAPDQL_PASSWD -t -q 2>&1 | tee -a ${LOG_DIR}/load.log
        date 2>&1 | tee -a ${LOG_DIR}/load.log
        echo "End loading ${t} ..." 2>&1 | tee -a ${LOG_DIR}/load.log
    done
    echo "Loading done!"
}

function runQuery(){
    query=$1
    echo "run query ${query}..." 2>&1 | tee -a ${LOG_DIR}/tpch_query${query}.log
    start=$(date +%s%3N)
    date 2>&1 | tee -a ${LOG_DIR}/tpch_query${query}.log
    $DBGEN_HOME/qgen ${query} | grep -v "^\s*$" | grep -v '^--' | grep -v 'LIMIT -' | tac | sed '0,/;/s///;1i;\' | tac | $MAPD_PATH/bin/mapdql -p $MAPDQL_PASSWD -t -q 2>&1 | tee -a ${LOG_DIR}/tpch_query${query}.log
    end=$(date +%s%3N)
    getExecTime $start $end >> ${LOG_DIR}/tpch_query${query}.log
    echo "query ${query} done!" 2>&1 | tee -a ${LOG_DIR}/tpch_query${query}.log
}

if [ "$LOAD" = true ]
then
    Load
fi

if [ -z "$QNUM" ]
then
    queries=$(seq 1 22)
else
    queries=$(seq $QNUM $QNUM)
fi

for n in $queries
do
    echo "===start run query $n==="
    date
    runQuery ${n}
    date
    echo "===end query $n ==="
done
