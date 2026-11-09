LINC_SIF=/var/scratch/kdirker/linc_latest.sif
LOCAL_MOUNT=/var/scratch/kdirker/master-thesis/data
echo "Run file is incomplete, exiting..."
exit
CONTAINER_MOUNT=
$CWL_OPTIONS=

singularity exec --bind <mount_points>:<mount_points> $LINC_SIF cwltool --no-container $CWL_OPTIONS /usr/local/share/linc/workflows/HBA_calibrator.cwl LINC.json