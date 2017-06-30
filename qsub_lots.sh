for i in *wet* 
do
    if test -f "$i" 
    then
       echo "Submitting $i to PBS"
       qsub $i
    fi
done
