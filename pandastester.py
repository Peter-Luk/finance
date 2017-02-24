from trial01 import I2
import pandas
mf = I2(code='mhih7')
i, r_date = 0, []
while i < len(mf._I2__data):
    if mf._I2__data[i]['date'] not in r_date:r_date.append(mf._I2__data[i]['date'])
    i += 1
data, hdr, dd = [], {}, {}
for i in r_date:
    hdr = mf._I2__rangefinder(field='date', value=i)['D']
    hdr['date'] = i
    data.append(hdr)
d_keys = list(data[0].keys())
for dk in d_keys:
    dd[dk.capitalize()] = [data[i][dk] for i in range(len(data))]
df = pandas.DataFrame(dd)
