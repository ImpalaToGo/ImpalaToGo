export M2_HOME=`mvn --version | grep "Maven home" | sed 's/^Maven home: //'`
export M2=$M2_HOME/bin
export JAVA_HOME=`mvn --version | grep "Java home" | sed 's/^Java home: //'`
export BOOST_ROOT=~/opt/boost
export LD_LIBRARY_PATH=$BOOST_ROOT/lib:$LD_LIBRARY_PATH

export PATH=$M2:$PATH

if [ "${PWD##/*/}" = "ImpalaToGo" ];
then
        export IMPALA_HOME=$PWD
else
        export IMPALA_HOME=$HOME/ImpalaToGo-repo/ImpalaToGo
fi

source $IMPALA_HOME/bin/impala-config.sh
