#!/bin/sh

VM_IDS=$(onevm list | awk 'FNR > 1 {print $1}' | paste -s -d ',')
read -p "This will terminate all running VMs with IDs $VM_IDS. Are you sure? (y/n): " choice
case "$choice" in
    y|Y) onevm terminate --hard $VM_IDS ;;
    *) exit 0 ;;
esac

