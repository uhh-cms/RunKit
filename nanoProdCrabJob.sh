#/bin/bash

prod_py="nanoProdCrabJob.py"
if ! [ -f "$prod_py" ] ; then
  prod_py="$CMSSW_BASE/src/nanoProdCrabJob.py"
fi
if ! [ -f "$prod_py" ] ; then
  prod_py="$ANALYSIS_PATH/RunKit/nanoProdCrabJob.py"
fi
if ! [ -f "$prod_py" ] ; then
  echo "ERROR: nanoProdCrabJob.py not found"
  exit 1
fi

python3 "$prod_py" "$@"
exit 0
