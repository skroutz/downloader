"""
Deploy Downloader with fabric

Note: Needs fabric 1.x

Usage:

$ ln -s contrib/fabfile.py
$ fab -H downloader1,downloader2 deploy

"""
from json import loads as json

from fabric.api import env, settings, parallel
from fabric.operations import put, sudo, local, run
from fabric.decorators import runs_once
from fabric.context_managers import hide

@parallel
def tail():
    env.remote_interrupt = True
    with settings(warn_only=True):
        run('journalctl --unit=downloader@* --follow --lines=0', pty=True)

@parallel
def version():
    run('/usr/local/lib/downloader/bin/downloader version')

@runs_once
def build():
    local('make docker-build')

def copy():
    put('docker-build/downloader', '/usr/local/lib/downloader/bin', use_sudo=True, mode=0755)
    put('utils/dlstats-graphite', '/usr/local/lib/downloader/bin', use_sudo=True, mode=0755)
    put('utils/purge-assets', '/usr/local/lib/downloader/bin', use_sudo=True, mode=0755)

def restart():
    sudo('systemctl restart downloader@api.service')
    sudo('systemctl restart downloader@processor.service')
    sudo('systemctl restart downloader@notifier.service')

def status():
    run('systemctl status downloader@*.service')

def stats():
    keys = ['processor', 'notifier']

    with hide('output'):
        stats = [json(run('redis-cli get stats:{}'.format(s))) for s in keys]
        for ss in stats:
            for s, v in json(ss).items():
                print "{:>30} {:>10}".format(s, v)

def deploy():
    build()
    copy()
    restart()
