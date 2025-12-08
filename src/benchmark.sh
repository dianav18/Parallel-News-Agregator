#!/bin/bash

ARTICLES="../checker/input/tests/test_5/articles.txt"
INPUTS="../checker/input/tests/test_5/inputs.txt"

OUTFILE="benchmark_results.csv"

echo "Threads,Time1,Time2,Time3,Average" > $OUTFILE

echo "Threads | Time1 (s) | Time2 (s) | Time3 (s) | Average (s)"
echo "----------------------------------------------------------"

for t in {1..8}; do
    declare -a times=()

    for run in 1 2 3; do
        output=$(java -jar Tema1.jar $t "$ARTICLES" "$INPUTS" 2>/dev/null | tail -n 1)
        time=$(echo "$output" | awk '{print $3}')
        time=${time//seconds/}
        times+=("$time")
    done

    avg=$(echo "scale=6; (${times[0]} + ${times[1]} + ${times[2]}) / 3" | bc)

    printf "%7d | %10s | %10s | %10s | %12s\n" \
        "$t" "${times[0]}" "${times[1]}" "${times[2]}" "$avg"

    echo "$t,${times[0]},${times[1]},${times[2]},$avg" >> $OUTFILE
done

echo ""
echo "Rezultatele au fost salvate în benchmark_results.csv"
