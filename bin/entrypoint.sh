#!/bin/bash

#
# This simple script is being used to spawn multiple processes per downloader container.
#
set -ueo pipefail

#. Start the API process
# NOTE: The port and host variables should be provided via environment variables.
/srv/downloader api --port 3000 --host 0.0.0.0 -c /srv/config.json &

#. Start the Processor process
/srv/downloader processor -c /srv/config.json &

#. Start the Notifier process
/srv/downloader notifier -c /srv/config.json &

#. Wait for all processes to exit
wait

#. Exit with the process status that exited first
exit $?
