#cp -f fe/* /usr/lib/impala/lib/
cp -f llvm-ir/*  /usr/lib/impala/llvm-ir/
cp -f be/* /usr/lib/impala/sbin/
sudo rm -rf /usr/lib/impala/impalatogoLib/*
sudo cp fe/*.jar /usr/lib/impala/lib/

sudo cp fe/dependency/* /usr/lib/impala/impalatogoLib/
sudo cp $(eval echo ~${SUDO_USER})/build/lib/* /usr/lib/impala/lib/
sudo ln -sfn /usr/lib/impala/lib/libboost_thread.so.1.54.0 /usr/lib/impala/lib/libboost_thread.so
sudo ln -sfn /usr/lib/impala/lib/libboost_regex.so.1.54.0 /usr/lib/impala/lib/libboost_regex.so
sudo ln -sfn /usr/lib/impala/lib/libboost_date_time.so.1.54.0 /usr/lib/impala/lib/libboost_date_time.so
sudo ln -sfn /usr/lib/impala/lib/libboost_filesystem.so.1.54.0  /usr/lib/impala/lib/libboost_filesystem.so
sudo ln -sfn /usr/lib/impala/lib/libboost_system.so.1.54.0 /usr/lib/impala/lib/libboost_system.so
sudo ln -sfn /usr/lib/impala/lib/libicuuc.so.52.1 /usr/lib/impala/lib/libicuuc.so.52
sudo ln -sfn /usr/lib/impala/lib/libicui18n.so.52.1 /usr/lib/impala/lib/libicui18n.so.52
sudo ln -sfn /usr/lib/impala/lib/libicudata.so.52.1 /usr/lib/impala/lib/libicudata.so.52

sudo chown -R impala  /usr/lib/impala/
sudo chown -R impala  /usr/lib/impala/llvm-ir

