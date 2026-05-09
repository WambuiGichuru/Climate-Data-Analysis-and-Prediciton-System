#!/bin/bash
if [ -f .demo_pids ]; then
    while read pid; do kill "$pid" 2>/dev/null; done < .demo_pids
    rm .demo_pids
fi
docker compose down
echo "Demo stopped."
