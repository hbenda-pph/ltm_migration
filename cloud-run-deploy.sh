#!/bin/bash
# Script para desplegar el Cloud Run job

PROJECT_ID="platform-partners-pro"
REGION="us-central1"
JOB_NAME="ltm-migration-job"
IMAGE_NAME="gcr.io/$PROJECT_ID/ltm-migration"

echo "🚀 Desplegando LTM Migration Job..."

# Construir imagen
echo "📦 Construyendo imagen Docker..."
docker build -t $IMAGE_NAME .

# Subir imagen a Container Registry
echo "⬆️ Subiendo imagen a Container Registry..."
docker push $IMAGE_NAME

# Crear Cloud Run job
echo "🔧 Creando Cloud Run job..."
gcloud run jobs create $JOB_NAME \
    --image $IMAGE_NAME \
    --region $REGION \
    --project $PROJECT_ID \
    --set-env-vars GOOGLE_CLOUD_PROJECT=$PROJECT_ID

echo "✅ Cloud Run job desplegado exitosamente!"
echo "🎯 Para ejecutar: gcloud run jobs execute $JOB_NAME --region $REGION" 