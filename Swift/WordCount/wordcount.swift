type file;

app (file out) wordcount (file input)
{
    wc "-w"  @input stdout=@out;
}


file inputs[]    <filesys_mapper; location="inputs", suffix="txt">;


foreach input,i in inputs
{
  file out <single_file_mapper; file=strcat("output/wordcount-swift.txt")>;
  out = wordcount (input);
}

