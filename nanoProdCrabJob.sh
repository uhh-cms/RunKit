#/bin/bash

prod_sh_dir="$(dirname "$0")"
prod_py="$prod_sh_dir/nanoProdCrabJob.py"
if ! [ -f "$prod_py" ] ; then
  echo "ERROR: nanoProdCrabJob.py not found"
  exit 1
fi

echo "which python"
which python3
if [ "x$1" = "xDEBUG" ] ; then
  python3 -u "$prod_py" "${@:2}"
else
  python3 -u "$prod_py"
fi

exit 0
