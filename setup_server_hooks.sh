#!/usr/bin/env bash

# Requirement: Git 2.3
# https://github.com/blog/1957-git-2-3-has-been-released

git config receive.denyCurrentBranch updateInstead


REPDIR=`git rev-parse --show-toplevel`
HOOKDIR=$REPDIR/.git/hooks

# set-up update hook
if [ ! -L $HOOKDIR/update ]; then
	ln -s -f ../../.hooks/update $HOOKDIR/update
fi
