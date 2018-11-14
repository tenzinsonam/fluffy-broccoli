pip install pymemcache
pip install mysql-connector-python
pip install pymysql
pip install mysql-connector-python

# Uncomment For Debian Distribution
# sudo pacman -S mysql-python
# sudo pacman -S mariadb-all
# sudo pacman mariadb
# sudo pacman -S memcached

# change permission bits for python in /usr/lib



# MYSQL RUN
systemctl enable mysqld.service
sudo mysql_install_db --user=mysql --basedir=/usr --datadir=/var/lib/mysql
sudo systemctl start mysqld
sudo mysql_secure_installation
mysql -u root -p

# Password of MYSQL is password


# LINK for dumping and restoring dump.sql 
#NOTE Create Database cs632 if not already there on the new machine
#https://www.liquidweb.com/kb/how-to-back-up-mysql-databases-from-the-command-line/

#CREATE USER 'root'@'localhost' IDENTIFIED BY 'password';
#GRANT ALL PRIVILEGES ON * . * TO 'root'@'localhost';
