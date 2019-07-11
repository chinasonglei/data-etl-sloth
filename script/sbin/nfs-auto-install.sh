#!/usr/bin/env bash


flag=$(rpm -qa | grep -i nfs-* | wc -l)
if [ "$flag" -le "0" ];then
   yum install nfs-utils nfs-utils-lib rpcbind
fi


function enable_server() {
    systemctl enable rpcbind
    systemctl enable nfs-server
    systemctl enable nfs-lock
    systemctl enable nfs-idmap
}

function start_server() {
    systemctl start rpcbind
    systemctl start nfs-server
    systemctl start nfs-lock
    systemctl start nfs-idmap
}


function re_start_server() {
    systemctl restart rpcbind
    systemctl restart nfs-server
    systemctl restart nfs-lock
    systemctl restart nfs-idmap
}

function status_server() {
    systemctl status rpcbind
    systemctl status nfs-server
    systemctl status nfs-lock
    systemctl status nfs-idmap
}


function stop_server() {
    systemctl stop rpcbind
    systemctl stop nfs-server
    systemctl stop nfs-lock
    systemctl stop nfs-idmap
}

function addPath() {
    echo "/opt    172.17.0.0/24(rw,sync,no_root_squash,no_subtree_check)" >> /etc/exports
    exportfs -r
 }