#!/bin/bash

# Find the Airflow webserver container
CONTAINER_ID=$(docker ps | grep "airflow-webserver" | awk '{print $1}')

if [ -z "$CONTAINER_ID" ]; then
    echo "Error: Airflow webserver container not found!"
    echo "Make sure your Airflow is running."
    exit 1
fi

echo "Found Airflow webserver container: $CONTAINER_ID"

# Create local directory for documentation
echo "Creating directory for documentation..."
mkdir -p ./dbt_docs

# Copy documentation files from container
echo "Copying documentation files from container..."
docker cp $CONTAINER_ID:/opt/airflow/dbt/target/index.html ./dbt_docs/
docker cp $CONTAINER_ID:/opt/airflow/dbt/target/catalog.json ./dbt_docs/
docker cp $CONTAINER_ID:/opt/airflow/dbt/target/manifest.json ./dbt_docs/

# Check if the files were copied successfully
if [ -f "./dbt_docs/index.html" ]; then
    echo "Documentation copied successfully!"
    echo ""
    echo "To view the documentation, open this file in your browser:"
    echo "$(pwd)/dbt_docs/index.html"
    
    # Try to open the documentation automatically
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        open ./dbt_docs/index.html
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        if command -v xdg-open > /dev/null; then
            xdg-open ./dbt_docs/index.html
        else
            echo "To open, run: your-browser $(pwd)/dbt_docs/index.html"
        fi
    elif [[ "$OSTYPE" == "cygwin" ]] || [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
        # Windows
        start ./dbt_docs/index.html
    fi
else
    echo "Error: Failed to copy documentation files."
    echo "Please make sure the dbt_documentation DAG has been run successfully."
fi 