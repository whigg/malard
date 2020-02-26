import os
import sys


def main(script, processes):
    months = range(1,12+1)
    years = [2011,2012,2019,2020]
    
    monthsandyears = sum([ [ (m,y) for m in months ]  for y in years ],[])
    
    fmt_cmd =  '"python {script}.py {month} {year} >> log/{script}_{year}_{month}.txt"'

    cmds = [  fmt_cmd.format(script=script, month=m,year=y) for m,y in monthsandyears ]

    commandString = " ".join( cmds )

    command = 'parallel -j {} ::: {}'.format(processes,commandString)

    os.system(command)

if __name__ == "__main__":
    
    args = sys.argv[1:]
    script = args[0]
    processes = int(args[1])
    
    main(script, processes )