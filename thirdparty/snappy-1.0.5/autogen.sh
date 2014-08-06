#! /bin/sh -e
rm -rf autom4te.cache
libtoolize --copy --force
aclocal -I m4 --force
autoheader --force
automake --add-missing --copy
autoconf --force

