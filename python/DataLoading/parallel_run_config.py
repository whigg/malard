import os
import sys


def main(script, processes, monthsandyears, processingRequest):
    
    configPath = os.path.join(processingRequest.resultPath, processingRequest.name)
    print("Config path is: {}".format(configPath) )
    fmt_cmd =  '"python {script}.py {month} {year} {configPath} >> log/{script}_{year}_{month}_{configName}.txt"'

    cmds = [  fmt_cmd.format(script=script, month=m,year=y, configPath=configPath, configName=processingRequest.name) for m,y in monthsandyears ]

    commandString = " ".join( cmds )

    command = 'parallel -j {} ::: {}'.format(processes,commandString)

    os.system(command)

if __name__ == "__main__":
    
    args = sys.argv[1:]
    script = args[0]
    processes = int(args[1])
    
    main(script, processes )