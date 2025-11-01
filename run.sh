#! /bin/bash

start_date="2025-02-01"
end_date="2025-04-10"

current_date="$start_date"
while [ "$current_date" != "$end_date" ]; do
    echo ./report.sh "$current_date"
    current_date=$(date -d "$current_date + 1 day" +%Y-%m-%d)
done
./report.sh "$end_date"