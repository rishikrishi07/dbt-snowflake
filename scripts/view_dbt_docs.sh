#!/bin/bash

# Get the container ID of the Airflow webserver
CONTAINER_ID=$(docker ps | grep "airflow-webserver" | awk '{print $1}')

if [ -z "$CONTAINER_ID" ]; then
    echo "Error: Airflow webserver container not found!"
    echo "Make sure your Airflow is running."
    exit 1
fi

echo "Found Airflow webserver container: $CONTAINER_ID"
echo "Starting documentation server..."

# Check if port 8081 is already in use
if netstat -tuln | grep -q ":8081 "; then
    echo "Warning: Port 8081 is already in use. You may need to kill that process first."
    echo "Run: lsof -i :8081 to find the process, then kill -9 <PID> to terminate it."
    echo "Attempting to start the server anyway..."
fi

# Start the documentation server
echo "Starting documentation server in the container..."
docker exec -d $CONTAINER_ID bash -c "cd /opt/airflow/dbt_docs && python -m http.server 8081"

# Check if server started
sleep 2
if docker exec $CONTAINER_ID bash -c "ps aux | grep -v grep | grep 'python -m http.server 8081'" > /dev/null; then
    echo "Documentation server started successfully!"
    echo ""
    echo "Access the dbt documentation at: http://localhost:8081"
    echo "When you're done, run: docker exec $CONTAINER_ID pkill -f 'python -m http.server 8081'"
    echo ""
else
    echo "Failed to start documentation server."
    echo "Try running this command manually:"
    echo "docker exec -it $CONTAINER_ID bash -c \"cd /opt/airflow/dbt_docs && python -m http.server 8081\""
fi 