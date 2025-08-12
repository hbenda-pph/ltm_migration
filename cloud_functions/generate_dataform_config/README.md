gcloud functions deploy generate_dataform_config \
  --runtime python310 \
  --trigger-http \
  --entry-point dataform_replication_handler \
  --memory 512MB \
  --timeout 540s \
  --set-env-vars PROJECT_SOURCE=tu-proyecto-metadata