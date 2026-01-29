#!/bin/bash
# Cron job: runs consolidation every 30 minutes
# Usage: Add this to your crontab with `crontab -e`
#
# */30 * * * * /home/dominic/Desktop/spark_practice/jobs/run_batch_job.sh

set -e

LOG_DIR="/home/dominic/Desktop/spark_practice/logs"
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/consolidation_$(date +\%Y\%m\%d_\%H\%M\%S).log"

echo "===== Batch Job Started: $(date) =====" >> "$LOG_FILE"

cd /home/dominic/Desktop/spark_practice

# Run the consolidation job
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  jobs/batch_consolidate_orders.py >> "$LOG_FILE" 2>&1

RESULT=$?

if [ $RESULT -eq 0 ]; then
  echo "===== Batch Job Succeeded: $(date) =====" >> "$LOG_FILE"
else
  echo "===== Batch Job Failed (exit code: $RESULT): $(date) =====" >> "$LOG_FILE"
fi

exit $RESULT
