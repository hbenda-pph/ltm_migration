import os
import json
from google.cloud import bigquery
import jinja2

# Configuración
COMPANIES_PROJECT = "constant-height-455614-i0"
COMPANIES_DATASET = "settings"
COMPANIES_TABLE = "companies"
TEMPLATE_DIR = "definitions/views"
OUTPUT_DIR = "definitions/generated"

def get_company_configs():
    """Obtiene configuración completa de cada compañía activa"""
    client = bigquery.Client(project=COMPANIES_PROJECT)
    query = f"""
        SELECT company_id
             , company_name
             , company_project_id
             , company_ltm_status
          FROM `{COMPANIES_PROJECT}.{COMPANIES_DATASET}.{COMPANIES_TABLE}`
         WHERE company_ltm_status = 0
         ORDER BY company_id"""
    return list(client.query(query).result())

def generate_view_files(configs):
    """Genera archivos SQLX específicos para cada compañía"""
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_DIR))
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for config in configs:
        context = {
            "PROJECT_ID": config.project_id,
            "COMPANY_ID": config.company_id,
            "RAW_DATASET": config.raw_dataset,
            "TARGET_DATASET": "silver"
        }
        
        for view_file in os.listdir(TEMPLATE_DIR):
            if view_file.endswith(".sqlx"):
                template = env.get_template(view_file)
                output = template.render(**context)
                
                output_filename = f"{OUTPUT_DIR}/{config.company_id}_{view_file}"
                with open(output_filename, "w") as f:
                    f.write(output)
                print(f"Generated: {output_filename}")

def generate_dataform_json(configs):
    """Genera dataform.json con variables específicas por compañía"""
    config = {
        "vars": {},
        "generation": []
    }
    
    for company in configs:
        config["generation"].append({
            "name": f"company_{company.company_id}",
            "vars": {
                "project_id": company.company_project_id,
                "company_id": company.company_id,
                "raw_dataset": f"servicetitan_{company.company_project_id.replace("-","_")}",
                "target_dataset": "silver"
            }
        })
    
    with open("dataform.json", "w") as f:
        json.dump(config, f, indent=2)
    print("Generated dataform.json with company-specific variables")

def run_dataform():
    """Ejecuta Dataform con las configuraciones generadas"""
    os.system("dataform compile")
    os.system("dataform run --actions *")  # Ejecuta todas las acciones generadas

def main():
    # Obtener configuraciones de todas las compañías
    company_configs = get_company_configs()
    
    # Generar archivos SQLX para cada compañía
    generate_view_files(company_configs)
    
    # Generar dataform.json con variables dinámicas
    generate_dataform_json(company_configs)
    
    # Ejecutar Dataform
    run_dataform()

if __name__ == "__main__":
    main()