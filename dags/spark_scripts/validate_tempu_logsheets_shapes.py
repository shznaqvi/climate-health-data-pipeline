# Extract summary statistics from quarantine_df
total_count = raw_df.count()
clean_count = clean_df.count()
quarantine_count = quarantine_df.count()

# Count error frequencies across all quarantined rows
error_breakdown = (
    quarantine_df.withColumn("reason", F.explode(F.col("error_reasons")))
    .groupBy("reason")
    .count()
    .rdd.collectAsMap()
)

metrics = {
    "total_rows": total_count,
    "clean_rows": clean_count,
    "quarantine_rows": quarantine_count,
    "error_breakdown": error_breakdown,
}

# Write summary metrics to temporary shared path for Airflow reporting task
with open(f"/tmp/audit_metrics_{execution_date}.json", "w") as f:
    json.dump(metrics, f)
