# low_altitude_governance

# flink路径：D:\Python\Python38\Lib\site-packages\pyflink，配置FLINK HOME
# 打开Jupyter
#关于mobaxterm，ip: 192.168.200.138 端口号：3306
连接vpn后，输入
sudo firewall-cmd --zone=public --add-port=10002（8888）/tcp --permanent
sudo firewall-cmd --reload
jupyter notebook --allow-root --ip=0.0.0.0 --no-browser
#调用服务器的jupyter调试，在这个文件夹/home/app/jupyter_files/test/
