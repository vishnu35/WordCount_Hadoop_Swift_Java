type file;

app (file output) sortdata (file unsorted)
{
  sort "-n" "small-dataset.txt" stdout=filename(output);
}

file unsorted <"small-dataset.txt">;
file sorted <"sort1MB -swift.txt">;

sorted = sortdata(unsorted);
