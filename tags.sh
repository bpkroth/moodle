#!/bin/bash
# See Also: tags.txt
dirpath=`pwd`
dirname=`basename "$dirpath"`
if [ "$dirname" == 'moodle' ] || [ "$dirname" == 'moodle.bpkroth' ]; then
	ctags -R --languages=php --exclude="CVS" --php-kinds=f \
		--regex-PHP='/abstract class ([^ ]*)/\1/c/' \
		--regex-PHP='/interface ([^ ]*)/\1/c/' \
		--regex-PHP='/(public |static |abstract |protected |private )+function ([^ (]*)/\2/f/'
else
	echo 'Wrong directory!' >&2
fi
