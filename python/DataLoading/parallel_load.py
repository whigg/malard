import os

years = range(2010,2019+1)

fmt_cmd =  '"python demdiffMadNew.py {year} >> log/Log_{year}.txt"'

cmds = [  fmt_cmd.format(year=y) for y in years ]

commandString = " ".join( cmds )

command = 'parallel -j {} ::: {}'.format(3,commandString)

os.system(command)
