sudo rm -rf /var/cache/impalatogo
sudo mkdir /media/raid0/cache
sudo ln -s /media/raid0/cache /var/cache/impalatogo
sudo chown -R impala /var/cache/impalatogo
sudo chown -R impala /media/raid0/cache
