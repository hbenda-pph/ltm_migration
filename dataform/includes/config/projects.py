# Configuración de proyectos BigQuery en Python
from typing import Dict, Optional, List
from google.cloud import bigquery
import os

# Estados de replicación
REPLICATION_STATUS = {
    -1: "NO_REPLICATE",      # No replicar
    0: "PENDING",            # Nunca ha replicado y lo requiere
    1: "SUCCESS",            # Replicado con éxito
    2: "ERROR",              # Replicado con error
    3: "IN_PROGRESS",        # Replicación en progreso
    4: "ROLLBACK",           # Rollback requerido
    5: "DEPRECATED"          # Compañía deprecada
}

class ProjectManager:
    """
    Gestor de proyectos que lee configuración desde BigQuery.
    """
    
    def __init__(self, source_project_id: str = None):
        """
        Inicializa el gestor de proyectos.
        
        Args:
            source_project_id: ID del proyecto que contiene la tabla companies
        """
        self.client = bigquery.Client()
        # Usar el proyecto QA como fuente de configuración
        self.source_project_id = source_project_id or "platform-partners-qua"
        
    def get_companies_from_bigquery(self, status_filter: Optional[int] = None) -> List[Dict]:
        """
        Obtiene las compañías desde BigQuery con filtro de estado opcional.
        
        Args:
            status_filter: Estado específico a filtrar (opcional)
            
        Returns:
            Lista de diccionarios con información de compañías
        """
        query = f"""
        SELECT 
            company_id,
            company_name,
            project_id,
            location,
            company_ltm_status,
            bronze_dataset,
            silver_dataset,
            gold_dataset,
            created_at,
            updated_at
        FROM `{self.source_project_id}.settings.companies`
        WHERE company_ltm_status IS NOT NULL
        """
        
        if status_filter is not None:
            query += f" AND company_ltm_status = {status_filter}"
        
        query += " ORDER BY company_name"
        
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            companies = []
            for row in results:
                companies.append({
                    "company_id": row.company_id,
                    "company_name": row.company_name,
                    "project_id": row.project_id,
                    "location": row.location or "US",
                    "company_ltm_status": row.company_ltm_status,
                    "datasets": {
                        "bronze": row.bronze_dataset or "bronze",
                        "silver": row.silver_dataset or "silver",
                        "gold": row.gold_dataset or "gold"
                    },
                    "created_at": row.created_at,
                    "updated_at": row.updated_at
                })
            
            return companies
            
        except Exception as e:
            print(f"❌ Error obteniendo compañías desde BigQuery: {e}")
            return []
    
    def get_project_config(self, environment: str, company_id: str = None) -> Dict:
        """
        Obtiene la configuración de un proyecto por ambiente y compañía.
        
        Args:
            environment: Ambiente ('dev', 'qa', 'prod')
            company_id: ID de la compañía (requerido para 'prod')
        
        Returns:
            Dict con la configuración del proyecto
        
        Raises:
            ValueError: Si no se encuentra la configuración
        """
        if environment in ['dev', 'qa']:
            # Para dev y qa, usar configuración local
            return self._get_local_config(environment)
        elif environment == 'prod' and company_id:
            # Para producción, buscar en BigQuery
            companies = self.get_companies_from_bigquery()
            for company in companies:
                if company['company_id'] == company_id:
                    return {
                        "projectId": company['project_id'],
                        "location": company['location'],
                        "datasets": company['datasets'],
                        "company_info": company
                    }
            
            raise ValueError(f"Compañía {company_id} no encontrada en BigQuery")
        else:
            raise ValueError(f"Configuración no válida: environment={environment}, company_id={company_id}")
    
    def _get_local_config(self, environment: str) -> Dict:
        """Configuración local para dev y qa."""
        local_configs = {
            "dev": {
                "projectId": "platform-partners-dev",
                "location": "US",
                "datasets": {
                    "bronze": "bronze_dev",
                    "silver": "silver_dev",
                    "gold": "gold_dev"
                }
            },
            "qa": {
                "projectId": "platform-partners-qua",
                "location": "US",
                "datasets": {
                    "bronze": "bronze_qa",
                    "silver": "silver_qa",
                    "gold": "gold_qa"
                }
            }
        }
        return local_configs.get(environment, {})
    
    def get_companies_by_status(self, status: int) -> List[Dict]:
        """
        Obtiene compañías por estado de replicación.
        
        Args:
            status: Estado de replicación
            
        Returns:
            Lista de compañías con ese estado
        """
        return self.get_companies_from_bigquery(status)
    
    def get_companies_to_replicate(self) -> List[Dict]:
        """
        Obtiene compañías que necesitan replicación (estado 0).
        
        Returns:
            Lista de compañías pendientes de replicación
        """
        return self.get_companies_by_status(0)
    
    def get_companies_with_errors(self) -> List[Dict]:
        """
        Obtiene compañías con errores de replicación (estado 2).
        
        Returns:
            Lista de compañías con errores
        """
        return self.get_companies_by_status(2)
    
    def update_company_status(self, company_id: str, new_status: int, error_message: str = None) -> bool:
        """
        Actualiza el estado de replicación de una compañía.
        
        Args:
            company_id: ID de la compañía
            new_status: Nuevo estado
            error_message: Mensaje de error (opcional)
            
        Returns:
            True si se actualizó correctamente
        """
        update_query = f"""
        UPDATE `{self.source_project_id}.settings.companies`
        SET 
            company_ltm_status = {new_status},
            last_replication_error = {f"'{error_message}'" if error_message else "NULL"},
            updated_at = CURRENT_TIMESTAMP()
        WHERE company_id = '{company_id}'
        """
        
        try:
            query_job = self.client.query(update_query)
            query_job.result()
            print(f"✅ Estado actualizado para {company_id}: {REPLICATION_STATUS[new_status]}")
            return True
        except Exception as e:
            print(f"❌ Error actualizando estado de {company_id}: {e}")
            return False
    
    def get_all_companies(self) -> List[Dict]:
        """Obtiene todas las compañías configuradas."""
        return self.get_companies_from_bigquery()
    
    def get_raw_dataset_name(self, project_id: str) -> str:
        """
        Genera el nombre del dataset raw basado en el project_id.
        Reemplaza guiones por guiones bajos SOLO en el nombre del dataset
        para cumplir con las reglas de BigQuery (los datasets no pueden tener guiones).
        
        Args:
            project_id: ID del proyecto (ej: "shape-mhs-1")
            
        Returns:
            Nombre del dataset raw (ej: "servicetitan_shape_mhs_1")
        """
        # Reemplazar guiones por guiones bajos SOLO en el nombre del dataset
        # El project_id se mantiene igual, solo el dataset necesita el cambio
        safe_dataset_name = project_id.replace("-", "_")
        return f"servicetitan_{safe_dataset_name}"
    
    def get_replication_summary(self) -> Dict:
        """
        Obtiene un resumen de estados de replicación.
        
        Returns:
            Dict con conteos por estado
        """
        query = f"""
        SELECT 
            company_ltm_status,
            COUNT(*) as count
        FROM `{self.source_project_id}.settings.companies`
        WHERE company_ltm_status IS NOT NULL
        GROUP BY company_ltm_status
        ORDER BY company_ltm_status
        """
        
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            summary = {}
            for row in results:
                status_name = REPLICATION_STATUS.get(row.company_ltm_status, f"UNKNOWN_{row.company_ltm_status}")
                summary[status_name] = row.count
            
            return summary
            
        except Exception as e:
            print(f"❌ Error obteniendo resumen: {e}")
            return {}

# Instancia global del gestor de proyectos
project_manager = ProjectManager()

# Funciones de conveniencia para compatibilidad
def get_project_config(environment: str, company_id: str = None) -> Dict:
    """Función de conveniencia para obtener configuración de proyecto."""
    return project_manager.get_project_config(environment, company_id)

def get_companies_to_replicate() -> List[Dict]:
    """Función de conveniencia para obtener compañías pendientes."""
    return project_manager.get_companies_to_replicate()

def get_all_companies() -> List[Dict]:
    """Función de conveniencia para obtener todas las compañías."""
    return project_manager.get_all_companies()

def update_company_status(company_id: str, new_status: int, error_message: str = None) -> bool:
    """Función de conveniencia para actualizar estado."""
    return project_manager.update_company_status(company_id, new_status, error_message) 