#!/bin/bash

set -e

# Configuration
VENV_PATH="/opt/pytorch/bin/activate"
VLLM_MODEL="/english-handwritten/data/models/gemma-3-12b-it"
VLLM_PORT=8001
AIRFLOW_DAG="document_extraction_v2"  # Change to "document_extraction_parallel" if preferred
LOG_DIR="/english-handwritten/tmp/extraction_logs"

# Create log directory
mkdir -p $LOG_DIR

echo "🚀 Starting Document Extraction Pipeline"
echo "========================================"

# 1. Activate virtual environment
echo "🔧 Activating virtual environment..."
source $VENV_PATH
echo "✅ Virtual environment activated"

# 2. Check if vLLM server is running
echo "🔍 Checking vLLM server status..."
if curl -s http://localhost:$VLLM_PORT/health > /dev/null 2>&1; then
    echo "✅ vLLM server is already running on port $VLLM_PORT"
else
    echo "🚀 Starting vLLM server..."
    nohup python -m vllm.entrypoints.openai.api_server \
        --model $VLLM_MODEL \
        --dtype bfloat16 \
        --tensor-parallel-size 4 \
        --host 0.0.0.0 \
        --port $VLLM_PORT \
        > $LOG_DIR/vllm_server.log 2>&1 &
    
    VLLM_PID=$!
    echo "📝 vLLM server started with PID: $VLLM_PID"
    
    # Wait for server to be ready
    echo "⏳ Waiting for vLLM server to be ready..."
    for i in {1..60}; do
        if curl -s http://localhost:$VLLM_PORT/health > /dev/null 2>&1; then
            echo "✅ vLLM server is ready!"
            break
        fi
        echo "   Attempt $i/60... waiting 10 seconds"
        sleep 10
    done
    
    # Final check
    if ! curl -s http://localhost:$VLLM_PORT/health > /dev/null 2>&1; then
        echo "❌ vLLM server failed to start. Check logs: $LOG_DIR/vllm_server.log"
        exit 1
    fi
fi

# 3. Test vLLM server with a simple request
echo "🧪 Testing vLLM server..."
TEST_RESPONSE=$(curl -s -X POST http://localhost:$VLLM_PORT/v1/chat/completions \
    -H "Content-Type: application/json" \
    -d '{
        "model": "'$VLLM_MODEL'",
        "messages": [{"role": "user", "content": "Hello, respond with just: OK"}],
        "max_tokens": 10
    }' | grep -o '"content":"[^"]*"' | head -1)

if [[ -n "$TEST_RESPONSE" ]]; then
    echo "✅ vLLM server test successful"
else
    echo "❌ vLLM server test failed"
    exit 1
fi

# 4. Initialize Airflow
echo "🔧 Initializing Airflow..."

# Set Airflow home and configuration
export AIRFLOW_HOME="$PWD/airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags"

if [[ ! -f "$AIRFLOW_HOME/airflow.db" ]]; then
    echo "📊 Initializing Airflow database..."
    airflow db init
else
    echo "✅ Airflow database already exists"
fi

# Check if DAG file exists
if [[ ! -f "$PWD/airflow/dags/extraction_dag.py" ]]; then
    echo "❌ DAG file not found at $PWD/airflow/dags/extraction_dag.py"
    exit 1
fi

# Start scheduler and webserver
echo "🔄 Starting Airflow scheduler..."
nohup airflow scheduler > $LOG_DIR/scheduler.log 2>&1 &

echo "🌐 Starting Airflow webserver..."
nohup airflow webserver --port 8080 > $LOG_DIR/webserver.log 2>&1 &

# Wait for services to start
sleep 15

# Parse DAGs to ensure they're loaded
echo "📋 Parsing DAGs..."
airflow dags reserialize

# Start scheduler to process queue
echo "🔄 Starting Airflow scheduler..."
nohup airflow scheduler > $LOG_DIR/scheduler.log 2>&1 &
SCHEDULER_PID=$!
echo "📝 Scheduler started with PID: $SCHEDULER_PID"

# Wait for scheduler to start
sleep 10

# List available DAGs to verify
echo "📋 Available DAGs:"
airflow dags list | grep -E "(document_extraction|extraction)" || echo "⚠️  No extraction DAGs found"

# 5. Check if database exists
DB_PATH="$HOME/english-handwritten/data/db/extraction.db"
if [[ ! -f "$DB_PATH" ]]; then
    echo "⚠️  Database not found. Creating directory..."
    mkdir -p "$HOME/english-handwritten/data/db"
    echo "📝 Database will be created during first extraction at $DB_PATH"
fi

# 6. Trigger Airflow DAG
echo "🎯 Triggering Airflow DAG: $AIRFLOW_DAG"
airflow dags trigger $AIRFLOW_DAG

# 7. Monitor DAG execution
echo "📊 Monitoring DAG execution..."

# Simple monitoring without JSON parsing
for i in {1..30}; do
    STATE=$(airflow dags state $AIRFLOW_DAG 2>/dev/null | tail -1 | awk '{print $NF}' || echo "unknown")
    echo "   Status check $i/30: $STATE"
    
    case $STATE in
        "success")
            echo "🎉 DAG execution completed successfully!"
            break
            ;;
        "failed")
            echo "❌ DAG execution failed. Check Airflow UI for details."
            exit 1
            ;;
        "running"|"queued")
            echo "   DAG is $STATE..."
            sleep 30
            ;;
        *)
            echo "   DAG state: $STATE"
            sleep 30
            ;;
    esac
done

# 8. Show results summary
echo "📈 Checking extraction results..."
if [[ -f "$DB_PATH" ]]; then
    TOTAL_DOCS=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM documents;" 2>/dev/null || echo "0")
    INDEX1_ENTRIES=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM index1_entries;" 2>/dev/null || echo "0")
    INDEX2_ENTRIES=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM index2_entries;" 2>/dev/null || echo "0")
    
    echo "✅ Extraction Summary:"
    echo "   📄 Total documents processed: $TOTAL_DOCS"
    echo "   👥 INDEX I entries: $INDEX1_ENTRIES"
    echo "   🏠 INDEX II entries: $INDEX2_ENTRIES"
else
    echo "⚠️  Database not found - extraction may have failed"
fi

echo ""
echo "🏁 Pipeline execution completed!"
echo "📊 Airflow UI: http://3.83.158.201:8080 (external) or http://localhost:8080 (local)"
echo "🔍 vLLM logs: $LOG_DIR/vllm_server.log"
echo "📋 Scheduler logs: $LOG_DIR/scheduler.log"
echo "🌐 Webserver logs: $LOG_DIR/webserver.log"
echo "💾 Database: $DB_PATH"