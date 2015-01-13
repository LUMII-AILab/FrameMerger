#/bin/sh
../SemanticAnalyzer/CVFactData/run.sh

pg_dump --format custom --blobs --verbose leta_cv2 > "../letacv2_$(date +"%Y%m%d").backup"
./entity_maintenance.py --cleardb

LOG="../error_log$(date +"%Y%m%d").txt"
./cvupload.sh 2> $LOG
# delete errorlog if empty
if [ ! -s $LOG ] ; then
  rm $LOG
fi
