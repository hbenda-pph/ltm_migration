### Cloud Functions Deploy :

cd cloud_functions/generate_dataform_config

gcloud functions deploy generate_dataform_config \
  --runtime python310 \
  --trigger-http \
  --entry-point dataform_replication_handler \
  --memory 512MB \
  --timeout 540s \
  --set-env-vars PROJECT_SOURCE=constant-height-455614-i0

#NEW 

gcloud functions deploy generate_dataform_config \
  --runtime python310 \
  --trigger-http \
  --entry-point dataform_replication_handler \
  --memory 512MB \
  --timeout 540s \
  --region us-central1 \
  --set-env-vars PROJECT_SOURCE=constant-height-455614-i0 \
  --allow-unauthenticated