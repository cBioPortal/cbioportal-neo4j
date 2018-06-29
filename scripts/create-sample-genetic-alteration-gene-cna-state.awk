# for each incoming cna event, create the proper gene-cna state node (target)

BEGIN {
FS="\t";
}
{
    if ($2 >= -2 && $2 <= 2)
        printf("%s\t%s\t%s_%s\t%s\n", $1, $2, $3, $2, $4)
}
