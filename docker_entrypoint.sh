#!/bin/bash

service dbus start
bluetoothd &
tail -f /dev/null

/bin/bash