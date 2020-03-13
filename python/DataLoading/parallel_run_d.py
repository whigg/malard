import os
import sys


def main(script, processes):
    #months = range(1,12+1)
    #years = [2011,2019]

    #monthsandyears = sum([ [ (m,y) for m in months ]  for y in years ],[])

    monthsandyears = [(2,2011),(3,2011),(4,2011),(5,2011),(6,2019),(7,2019),(8,2019),(9,2019),(10,2019),(11,2019)]
    
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