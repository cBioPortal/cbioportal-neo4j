# for each incoming cna event, create the proper gene-cna target node

BEGIN {
FS="\t";
}
{
    split($1, parts, "_")
    printf("%s\t%s\tchild_of_gene\n", $1, parts[1] )
}
