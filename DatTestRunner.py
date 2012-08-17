from Core import DatFile
from Core import OutLierRejection
import glob, time, numpy
from Core import AverageList

def print_timing(func):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        res = func(*args, **kwargs)
        t2 = time.time()
        print '%s took %0.3f ms' % (func.func_name, (t2-t1)*1000.0)
        return res
    return wrapper


averageList = AverageList.AverageList()
rejection = OutLierRejection.OutLierRejection()
buff = [ DatFile.DatFileSpeedTest(dat) for dat in glob.glob('data/dat/*GIbuffer_1*.dat') ]
samp = [ DatFile.DatFileSpeedTest(dat) for dat in glob.glob('data/dat/*GI25*.dat') ]


#@print_timing
#def nath_average(lzt):
#    return [sum(item) / len(item) for item in zip(*lzt)]
#
#@print_timing
#def jack_average(lzt):
#    return averageList.average(lzt)
#
#@print_timing
#def jack_subtract(buf, intensities):
#    newIntensities = []
#    for i in range(0, len(intensities)):
#        newIntensities.append(intensities[i] - buf[i])
#    return newIntensities
#
#@print_timing
#def nath_subtract(buff, samp):
#    return [ val - buff[i] for i, val in enumerate(samp) ]
@print_timing
def get_i(lzt):
    return zip(*[ dat.getIntensities() for dat in lzt ])
@print_timing
def nath_average(lzt):
    return DatFile.average(lzt)
@print_timing
def nath_subtract(samp, buff):
    return samp.subtract(buff)

#get_i(buff)
#get_i(samp)
rejection.process(buff)
buffname = buff[0].getBaseFileName()
buff = nath_average([ dat for dat in buff if dat.isValid() ])
fname = samp[0].getBaseFileName()
rejection.process(samp)
samp = nath_average([ dat for dat in samp if dat.isValid() ])
subtracted = nath_subtract(samp, buff)
buff.write('buff_%s' % buffname)
subtracted.write('avg_sub_%s' % fname)
#av1 = jack_average(intensities)
#av2 = nath_average(intensities)
#print av1 == av2
