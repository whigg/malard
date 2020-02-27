import os
import sys

def main( script, run_name, processors ):
    years = range(2014,2016+1)
    
    months = range(1,12+1)
    
    yearsandmonths = sum([[(month,year) for month in months ] for year in years ],[])
    
    fmt_cmd =  '"python3 {script}.py {month} {year} >> log/Log_{run_name}_{year}_{month}.txt"'
       
    cmds = [  fmt_cmd.format(month=m, year=y, script=script, run_name=run_name) for m,y in yearsandmonths ]
    
    commandString = " ".join( cmds )
    
    command = 'parallel -j {} ::: {}'.format(processors,commandString)
    
    os.system(command)

if __name__ == "__main__":
    args = sys.argv[1:]
    script = args[0]
    run = args[1]
    processors = args[2]
    main(script, run, int(processors) )