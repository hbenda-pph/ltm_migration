#!/usr/bin/env python3
"""
Scripts de monitoreo de calidad de datos para BigQuery.
"""

from google.cloud import bigquery
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime
import json
from pathlib import Path

class DataQualityMonitor:
    """
    Clase para monitorear la calidad de datos en BigQuery.
    """
    
    def __init__(self, project_id: str):
        """
        Inicializa el monitor de calidad.
        
        Args:
            project_id: ID del proyecto de BigQuery
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
    
    def check_data_quality(self, dataset: str, table: str) -> Dict:
        """
        Verifica la calidad de datos en una tabla específica.
        
        Args:
            dataset: Nombre del dataset
            table: Nombre de la tabla
            
        Returns:
            Dict con resultados de la verificación
        """
        print(f"🔍 Verificando calidad de datos en {dataset}.{table}")
        
        try:
            # Obtener información de la tabla
            table_ref = self.client.dataset(dataset).table(table)
            table_obj = self.client.get_table(table_ref)
            
            # Consultas de calidad
            quality_checks = {
                "total_rows": self._get_total_rows(dataset, table),
                "null_counts": self._get_null_counts(dataset, table),
                "duplicate_check": self._check_duplicates(dataset, table),
                "date_range": self._get_date_range(dataset, table),
                "column_info": self._get_column_info(table_obj)
            }
            
            # Evaluar resultados
            status = self._evaluate_quality(quality_checks)
            
            return {
                "dataset": dataset,
                "table": table,
                "timestamp": datetime.now().isoformat(),
                "status": status,
                "checks": quality_checks
            }
            
        except Exception as e:
            print(f"❌ Error verificando calidad: {e}")
            return {
                "dataset": dataset,
                "table": table,
                "timestamp": datetime.now().isoformat(),
                "status": "ERROR",
                "error": str(e)
            }
    
    def _get_total_rows(self, dataset: str, table: str) -> int:
        """Obtiene el total de filas en la tabla."""
        query = f"SELECT COUNT(*) as total FROM `{self.project_id}.{dataset}.{table}`"
        result = self.client.query(query).result()
        return next(result).total
    
    def _get_null_counts(self, dataset: str, table: str) -> Dict:
        """Obtiene conteos de valores nulos por columna."""
        # Obtener columnas de la tabla
        table_ref = self.client.dataset(dataset).table(table)
        table_obj = self.client.get_table(table_ref)
        
        null_counts = {}
        for field in table_obj.schema:
            query = f"""
            SELECT COUNT(*) as null_count 
            FROM `{self.project_id}.{dataset}.{table}` 
            WHERE {field.name} IS NULL
            """
            result = self.client.query(query).result()
            null_counts[field.name] = next(result).null_count
        
        return null_counts
    
    def _check_duplicates(self, dataset: str, table: str) -> Dict:
        """Verifica duplicados en la tabla."""
        # Obtener columnas de la tabla
        table_ref = self.client.dataset(dataset).table(table)
        table_obj = self.client.get_table(table_ref)
        
        # Usar todas las columnas para verificar duplicados
        columns = [field.name for field in table_obj.schema]
        columns_str = ", ".join(columns)
        
        query = f"""
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT {columns_str}) as unique_rows
        FROM `{self.project_id}.{dataset}.{table}`
        """
        result = self.client.query(query).result()
        row = next(result)
        
        return {
            "total_rows": row.total_rows,
            "unique_rows": row.unique_rows,
            "duplicate_count": row.total_rows - row.unique_rows
        }
    
    def _get_date_range(self, dataset: str, table: str) -> Dict:
        """Obtiene el rango de fechas si hay columnas de fecha."""
        # Buscar columnas de fecha
        table_ref = self.client.dataset(dataset).table(table)
        table_obj = self.client.get_table(table_ref)
        
        date_columns = []
        for field in table_obj.schema:
            if field.field_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                date_columns.append(field.name)
        
        date_ranges = {}
        for col in date_columns:
            query = f"""
            SELECT MIN({col}) as min_date, MAX({col}) as max_date
            FROM `{self.project_id}.{dataset}.{table}`
            WHERE {col} IS NOT NULL
            """
            result = self.client.query(query).result()
            row = next(result)
            date_ranges[col] = {
                "min_date": str(row.min_date) if row.min_date else None,
                "max_date": str(row.max_date) if row.max_date else None
            }
        
        return date_ranges
    
    def _get_column_info(self, table_obj) -> Dict:
        """Obtiene información de las columnas."""
        return {
            "total_columns": len(table_obj.schema),
            "columns": [field.name for field in table_obj.schema],
            "types": {field.name: field.field_type for field in table_obj.schema}
        }
    
    def _evaluate_quality(self, checks: Dict) -> str:
        """Evalúa la calidad basada en los checks."""
        issues = []
        
        # Verificar si hay filas
        if checks["total_rows"] == 0:
            issues.append("Tabla vacía")
        
        # Verificar duplicados
        if checks["duplicate_check"]["duplicate_count"] > 0:
            issues.append(f"Duplicados encontrados: {checks['duplicate_check']['duplicate_count']}")
        
        # Verificar valores nulos excesivos
        for col, null_count in checks["null_counts"].items():
            if null_count > checks["total_rows"] * 0.5:  # Más del 50% nulos
                issues.append(f"Columna {col} tiene muchos valores nulos: {null_count}")
        
        if issues:
            return "FAILED"
        else:
            return "PASSED"
    
    def generate_quality_report(self, dataset: str, tables: List[str]) -> Dict:
        """
        Genera un reporte de calidad para múltiples tablas.
        
        Args:
            dataset: Nombre del dataset
            tables: Lista de tablas a verificar
            
        Returns:
            Dict con el reporte completo
        """
        print("📊 Generando reporte de calidad...")
        
        report = {
            "project_id": self.project_id,
            "dataset": dataset,
            "timestamp": datetime.now().isoformat(),
            "tables": {}
        }
        
        for table in tables:
            result = self.check_data_quality(dataset, table)
            report["tables"][table] = result
        
        # Guardar reporte
        report_path = Path(f"quality_report_{dataset}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"📄 Reporte guardado en: {report_path}")
        return report

def main():
    """Función principal para ejecutar verificaciones de calidad."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Verificar calidad de datos en BigQuery")
    parser.add_argument("--project", required=True, help="ID del proyecto BigQuery")
    parser.add_argument("--dataset", required=True, help="Dataset a verificar")
    parser.add_argument("--table", help="Tabla específica a verificar")
    parser.add_argument("--all-tables", action="store_true", help="Verificar todas las tablas del dataset")
    
    args = parser.parse_args()
    
    monitor = DataQualityMonitor(args.project)
    
    if args.table:
        # Verificar tabla específica
        result = monitor.check_data_quality(args.dataset, args.table)
        print(json.dumps(result, indent=2))
    elif args.all_tables:
        # Verificar todas las tablas
        client = bigquery.Client(project=args.project)
        dataset_ref = client.dataset(args.dataset)
        tables = [table.table_id for table in client.list_tables(dataset_ref)]
        
        report = monitor.generate_quality_report(args.dataset, tables)
        print(f"✅ Reporte completado para {len(tables)} tablas")
    else:
        print("❌ Debes especificar --table o --all-tables")

if __name__ == "__main__":
    main() 