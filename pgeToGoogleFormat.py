#!/usr/bin/python 
# pgeToGoogleFormat.py
#
# Strips and reformats the .CSV files from PG&E's website into a
# format suitable to be piped into Google's "post_readings_devices.py"
# script.  The script should be run with the following options:
# --time_column=1 --reading_column=2 --durational --uncertainty=0.001 -b
#
# Specifically this script works only for PG&E *Hourly* measurements. I used
# it on the 'Weekly' downloads, but I think 'Daily' should work too.
#
# Bugs: 
# 
# 1) Assumes PST. PG&E's file has a "-" for the lost hour "Springing
# ahead".  I'm not sure what Google wants. I don't know what happens
# in Fall...
#
# 2) Requires the PG&E files to have "kWh" on the line before the
# measurements.
#
# 3) The PG&E script wants *accumulated* measurements, at least with
# the options I passed; this should be unnecessary.
#
# 4) Assumes hour-granularity. Ideally I'd parse the Time row I guess.

import os
import sys

if (len(sys.argv) < 2):
	print "Usage: %s <Filename.csv>" % sys.argv[0]
	sys.exit(0)
else:
	filename = sys.argv[1]


f = open(filename)

# Advance past all the headers
line = f.readline()
while line.count('"kWh"',0,5) == 0:
	line = f.readline()

line = f.readline()

# The real data starts here, parse it
accum = 0.0
while len(line) > 2:
	
	# Grab the date from the first column
	# and the measurements from the rest
	linesplit = line.split(',')
	linesplit.reverse()
	date = linesplit.pop()
	linesplit.reverse()
	readings = linesplit

	# Parse the date for RFC3339.
	# Sometimes PG&E has double quotes, sometimes not.
	(month,day,year) = date.split('/')
	month = month.replace('"','')
	year = year.replace('"','')
	day = day.replace('"','')

	# Output the date/time and *accumulated* measurement.
	hour = 0
	print "%s-%02d-%sT%02d:00:00-07:00   %f" % (year,int(month),day,hour,accum)
	for reading in readings:
		hour += 1
		accum += float(reading.replace('"','').replace('-','0'))
		if hour != 24:
			print "%s-%02d-%sT%02d:00:00-07:00   %f" % (year,int(month),day,hour, accum)
		else:
			print "%s-%02d-%sT23:59:59-07:00   %f" % (year,int(month),day,accum)
	line = f.readline()
f.close()

