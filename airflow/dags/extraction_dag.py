from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
sys.path.append('/english-handwritten/data')
from extractor import process_batch_documents, extract_document

def process_all_documents():
    """Process both INDEX I and INDEX II documents"""
    result = process_batch_documents("/english-handwritten/data/index1/", "/english-handwritten/data/index2/", delay_seconds=2, debug=False)
    print(f"✅ Batch processing complete!")
    print(f"INDEX I: {result['summary']['index1_successful']} success, {result['summary']['index1_failed']} failed")
    print(f"INDEX II: {result['summary']['index2_successful']} success, {result['summary']['index2_failed']} failed")
    return result

def process_index1_only():
    """Process only INDEX I documents"""
    import glob
    index1_files = glob.glob("/english-handwritten/data/index1/*.jpg") + glob.glob("/english-handwritten/data/index1/*.jpeg") + glob.glob("/english-handwritten/data/index1/*.png")
    results = []
    for file_path in sorted(index1_files):
        result = extract_document(file_path, "INDEX_1", debug=False)
        results.append(result)
        print(f"Processed: {os.path.basename(file_path)} - {'✅' if 'error' not in result else '❌'}")
    return results

def process_index2_only():
    """Process only INDEX II documents"""
    import glob
    index2_files = glob.glob("/english-handwritten/data/index2/*.jpg") + glob.glob("/english-handwritten/data/index2/*.jpeg") + glob.glob("/english-handwritten/data/index2/*.png")
    results = []
    for file_path in sorted(index2_files):
        result = extract_document(file_path, "INDEX_2", debug=False)
        results.append(result)
        print(f"Processed: {os.path.basename(file_path)} - {'✅' if 'error' not in result else '❌'}")
    return results

# Option 1: Process all documents in one task (RECOMMENDED)
with DAG(
    dag_id='document_extraction_v2',
    start_date=datetime(2025, 9, 10),
    schedule=None,
    catchup=False,
    description='Extract data from INDEX I and INDEX II historical documents'
) as dag:
    
    process_all_task = PythonOperator(
        task_id='process_all_documents',
        python_callable=process_all_documents
    )

# Option 2: Separate tasks (if you prefer parallel processing)
with DAG(
    dag_id='document_extraction_parallel',
    start_date=datetime(2025, 9, 10),
    schedule=None,
    catchup=False,
    description='Extract data from INDEX documents in parallel'
) as dag_parallel:
    
    task1 = PythonOperator(
        task_id='process_index1', 
        python_callable=process_index1_only
    )
    
    task2 = PythonOperator(
        task_id='process_index2', 
        python_callable=process_index2_only
    )
    
    # Run in parallel (no dependency)
    [task1, task2]
