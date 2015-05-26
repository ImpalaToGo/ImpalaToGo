#!/bin/bash

# This will starting or stopping tachyon master and worker locally
 case $1 in

    setup)

    echo "try tachyon starting..."
    
    file=./tachyon$IMPALA_HOME/thirdparty/tachyon/bin/tachyon

    if [[ -e $file  ]]   # check the presence of the file

     then $IMPALA_HOME/thirdparty/tachyon/bin/tachyon format

     else echo "file tachyon not found"

    fi

    file1=$IMPALA_HOME/thirdparty/tachyon/bin/tachyon-start.sh

    if [[ -e $file1  ]]

     then $IMPALA_HOME/thirdparty/tachyon/bin/tachyon-start.sh local

     else echo "file tachyon-start.sh not found"

    fi

    ;;
    
    teardown)

    echo "try tachyon stopping..."

    file=$IMPALA_HOME/thirdparty/tachyon/bin/tachyon-stop.sh

    if [[ -e $file  ]]

     then $IMPALA_HOME/thirdparty/tachyon/bin/tachyon-stop.sh all

     else echo "file tachyon-stop.sh not found"

    fi

    ;;

 esac







