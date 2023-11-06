#!/bin/sh

# -L returns 0 if there are any matches, which means any file containing the
# pragma will cause this to give a exit status of 1. It's a bit hacky, but we
# just pipe this to grep again, which exits with 0 if there are any lines and 1
# otherwise.
! grep -rL "^#pragma once" $1 | grep ""
