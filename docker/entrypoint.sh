#!/bin/sh
install -m 0600 /run/secrets/netrc ~/.netrc
growth-too $@
