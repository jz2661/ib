from ib_insync import *

util.startLoop()  # only use in interactive environments (i.e. Jupyter Notebooks)

ib = IB()
ib.connect(port=7496) # live

ib.positions()
