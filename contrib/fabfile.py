"""
Deploy Downloader with fabric

Usage:

$ ln -s contrib/fabfile.py
$ fab -H dl1,dl2 deploy

"""
from fabric.operations import put, sudo, local
from fabric.decorators import runs_once

@runs_once
def build():
    local('go build -o downloader')

def copy():
    put('downloader', '/usr/local/lib/downloader/bin', use_sudo=True, mode=0755)

def restart():
    sudo('systemctl restart downloader@api.service')
    sudo('systemctl restart downloader@processor.service')
    sudo('systemctl restart downloader@notifier.service')

def deploy():
    build()
    copy()
    restart()
