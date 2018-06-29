# for each incoming gene node, this script creates 5 gene nodes for each CN state

BEGIN {
FS="\t";
}
{
    for (lc = -2; lc <= 2; lc++)
        if (lc == -2) {
            printf("%s_%s\t%s\t%s_DEL\t%s\t%s_del\n", $1, lc, $1, $2, $2, $7)
        }
        else if (lc == -1) {
            printf("%s_%s\t%s\t%s_LOSS\t%s\t%s_loss\n", $1, lc, $1, $2, $2, $7)
        }
        else if (lc == 0) {
            printf("%s_%s\t%s\t%s_DIPLOID\t%s\t%s_diploid\n", $1, lc, $1, $2, $2, $7)
        }
        else if (lc == 1) {
            printf("%s_%s\t%s\t%s_GAIN\t%s\t%s_gain\n", $1, lc, $1, $2, $2, $7)
        }
        else if (lc == 2) {
            printf("%s_%s\t%s\t%s_AMP\t%s\t%s_amp\n", $1, lc, $1, $2, $2, $7)
        }
}
