CREATE USER 'hivetest'@'localhost' IDENTIFIED BY 'hivetest';
CREATE DATABASE hivetestdb DEFAULT CHARACTER SET latin1 DEFAULT COLLATE latin1_swedish_ci;
GRANT ALL PRIVILEGES ON hivetestdb.* TO 'hivetest'@'localhost' WITH GRANT OPTION;
GRANT FILE ON *.* TO 'hivetest'@'localhost' IDENTIFIED BY 'hivetest';
flush privileges;
