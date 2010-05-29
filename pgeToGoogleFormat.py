#!/usr/bin/python2.6 
# pgeToGoogleFormat.py
#
# Reads hourly-usage data from PG&E-provided .CSV files and uploads to
# Google PowerMeter using their Python API. The user of this script
# must:
# 
# 1) Download the Python API files from
# http://code.google.com/p/google-powermeter-api-client/downloads/list
# This script must be run from the same directory as google_meter.py
#
# 2) Follow the instructions at
# http://code.google.com/apis/powermeter/docs/powermeter_device_activation.html
# to get a "token" and "variable." Be sure to create a *Durational*
# variable (e.g. dvars=1). Note that on the final confirmation screen
# Google gives a "path" variable. The input to this script must be the variable.
# For example, Google says your path is "/user/1234/5678/variable/abcde"
# The variable is then "/user/1234/5678/variable/abcde.d1"
#
# 3) Download your data files from PG&E. If you can automate this, I'd
# love to get a script!
# 
# 4) Run those files through this script. Note that Google doesn't
# like you to upload too quickly. Ideally you should upload 1 csv file
# every 10 minutes. If you go faster, then eventually Google will
# block you for a while.
#
#
# Bugs:
# 
# 1) Currently the script does not upload any data for days that
# include a DST transition (1 day in Fall, 1 day in Spring). Framework
# for handling this is there, but I haven't gotten around to it yet.
#
#

import os
import sys
import csv
from datetime import tzinfo, timedelta, datetime, date, time
from optparse import OptionParser
import google_meter
import units
import rfc3339

def ParseArguments():
  op = OptionParser('%prog <token> <variable> <Filename.csv> \n\n' + '''
arguments:
  <token>               AuthSub token for GData requests (required)
  <variable>            entity path of a PowerMeter variable (required)
  <Filename.csv>        The Hourly usage CSV datafile from PG&E (required)''')
  op.add_option('', '--service', metavar='<URI>',
                help='URI prefix of the GData service to contact '
                     '(default: https://www.google.com/powermeter/feeds)')
  op.add_option('', '--unit', metavar='<symbol>',
                help='units of the measurements being posted (default: kW h)')
  op.add_option('', '--uncertainty', metavar='<uncertainty in kW h>',
                help='Uncertainty in measurements being posted (default: 0.001)')
  op.add_option('', '--time_uncertainty', metavar='<seconds>', type='float',
                help='uncertainty in measured times (default: 1)'),

  op.set_defaults(service='https://www.google.com/powermeter/feeds',
                  unit='kW h', uncertainty=0.001, time_uncertainty=1)

  # Parse and validate the command-line options.
  options, args = op.parse_args()
  if len(args) != 3:
    op.exit(1, op.format_help())
  return (args, options)

ZERO = timedelta(0)
HOUR = timedelta(hours=1)

def first_sunday_on_or_after(dt):
    days_to_go = 6 - dt.weekday()
    if days_to_go:
        dt += timedelta(days_to_go)
    return dt


# US DST Rules
#
# This is a simplified (i.e., wrong for a few cases) set of rules for US
# DST start and end times. For a complete and up-to-date set of DST rules
# and timezone definitions, visit the Olson Database (or try pytz):
# http://www.twinsun.com/tz/tz-link.htm
# http://sourceforge.net/projects/pytz/ (might not be up-to-date)
#
# In the US, since 2007, DST starts at 2am (standard time) on the second
# Sunday in March, which is the first Sunday on or after Mar 8.
DSTSTART_2007 = datetime(1, 3, 8, 2)
# and ends at 2am (DST time; 1am standard time) on the first Sunday of Nov.
DSTEND_2007 = datetime(1, 11, 1, 1)
# From 1987 to 2006, DST used to start at 2am (standard time) on the first
# Sunday in April and to end at 2am (DST time; 1am standard time) on the last
# Sunday of October, which is the first Sunday on or after Oct 25.
DSTSTART_1987_2006 = datetime(1, 4, 1, 2)
DSTEND_1987_2006 = datetime(1, 10, 25, 1)
# From 1967 to 1986, DST used to start at 2am (standard time) on the last
# Sunday in April (the one on or after April 24) and to end at 2am (DST time;
# 1am standard time) on the last Sunday of October, which is the first Sunday
# on or after Oct 25.
DSTSTART_1967_1986 = datetime(1, 4, 24, 2)
DSTEND_1967_1986 = DSTEND_1987_2006

class USTimeZone(tzinfo):

    def __init__(self, hours, reprname, stdname, dstname):
        self.stdoffset = timedelta(hours=hours)
        self.reprname = reprname
        self.stdname = stdname
        self.dstname = dstname

    def __repr__(self):
        return self.reprname

    def tzname(self, dt):
        if self.dst(dt):
            return self.dstname
        else:
            return self.stdname

    def utcoffset(self, dt):
        return self.stdoffset + self.dst(dt)

    def dst(self, dt):
        if dt is None or dt.tzinfo is None:
            # An exception may be sensible here, in one or both cases.
            # It depends on how you want to treat them.  The default
            # fromutc() implementation (called by the default astimezone()
            # implementation) passes a datetime with dt.tzinfo is self.
            return ZERO
        assert dt.tzinfo is self

        # Find start and end times for US DST. For years before 1967, return
        # ZERO for no DST.
        if 2006 < dt.year:
            dststart, dstend = DSTSTART_2007, DSTEND_2007
        elif 1986 < dt.year < 2007:
            dststart, dstend = DSTSTART_1987_2006, DSTEND_1987_2006
        elif 1966 < dt.year < 1987:
            dststart, dstend = DSTSTART_1967_1986, DSTEND_1967_1986
        else:
            return ZERO

        start = first_sunday_on_or_after(dststart.replace(year=dt.year))
        end = first_sunday_on_or_after(dstend.replace(year=dt.year))

        # Can't compare naive to aware objects, so strip the timezone from
        # dt first.
        if start <= dt.replace(tzinfo=None) < end:
            return HOUR
        else:
            return ZERO

Eastern  = USTimeZone(-5, "Eastern",  "EST", "EDT")
Central  = USTimeZone(-6, "Central",  "CST", "CDT")
Mountain = USTimeZone(-7, "Mountain", "MST", "MDT")
Pacific  = USTimeZone(-8, "Pacific",  "PST", "PDT")


def parseHeader(row):
  if row[0] == 'Title':
    if row[1] != 'Hourly Usage':
      print 'Input file is not "Hourly Usage"-type!'
      exit(1)
  elif row[0] == 'Primary Data Unit':
    if row[1] != 'kWh' and row[1] != 'k Wh':
      print 'Primary Data Unit is ' + row[1]
      print 'The only supported unit is kWh!'
      exit(1)
  elif row[0].startswith('Missing data'):
    handleMissingData(row)

def handleMissingData(row,days):
  print 'Warning: ' + row[0]
  print '\tThis is an unhandled special case, but it should still work. Expect usage to be 0'

def parseTimes(times):
  # Parse the times
  times.reverse()
  times.pop()
  times.reverse()

  convTimes = list()
  for timeElement in times:
    (timeElement,ampm) = timeElement.split(' ')
    colonCount = timeElement.count(':')
    if colonCount == 1:
      (hour,minute) = timeElement.split(':')
      second = 0
    else:
      (hour,minute,second) = timeElement.split(':')
      second = int(second)

    hour = int(hour)
    minute = int(minute)

    if ampm == 'AM' and hour == 12:
      convTimes.append(time(0,minute,second,tzinfo=Pacific))
    else:
      if ampm == 'AM' or hour == 12:
        convTimes.append(time(hour,minute,second,tzinfo=Pacific))
      else:
        # We are PM, and not noon
        convTimes.append(time(hour+12,minute,second,tzinfo=Pacific))

  d = date(2010,1,2)
  diff = datetime.combine(d,convTimes[1]) - datetime.combine(d,convTimes[0])
  i = 1
  while i < len(convTimes):
    if (datetime.combine(d,convTimes[i]) - datetime.combine(d,convTimes[i - 1])) != diff:
      print 'Warning: Unexpected difference in the times between each reading. Please upload your input file to the project website.'
      print '\tProcessing will continue, but the final timeslot\'s endtime may be incorrect.'
    i += 1
  return convTimes

def parseDay(row):
  # Grab the date from the first column
  # and the measurements from the rest
  row.reverse()
  datestr = row.pop()
  row.reverse()

  readings = list()
  for reading in row:
    if reading != '-':
      readings.append(float(reading))
    else:
      readings.append(float(0))

  # Parse the date
  # Sometimes PG&E has double quotes, sometimes not.
  (month,day,year) = datestr.split('/')
  month = month.replace('"','')
  year = year.replace('"','')
  day = day.replace('"','')
	
  d = date(int(year),int(month),int(day))
  return Day(d, readings)

class Day:
  """Simple struct to hold a date and the electricity readings.
  Members:
  day (datetime.date)
  readings (list(float))"""

  def __init__(self, day, readings):
    self.day = day
    self.readings = readings

class DurationalMeasurement:
  """Struct to hold everything we need to know about a durational measurement.
  Members:
  dStart: (datetime.datetime)\tStart of the duration
  dEnd: (datetime.datetime)\tEnd of the duration
  energy: (float)\tAmount of energy used in the duration (kWh)
  uncertainty: (float)\tUncertainty in energy"""

  defaultUncertainty = 0.001

  def __init__(self, dStart, dEnd, energy):
    self.dStart = dStart
    self.dEnd = dEnd
    self.energy = energy
    self.uncertainty = self.defaultUncertainty

  def setUncertainty(self, uncertainty):
    self.uncertainty = uncertainty

def isDSTBoundary(day):
  dBefore = datetime(day.year,day.month,day.day,0,30,tzinfo=Pacific)
  dAfter = datetime(day.year,day.month,day.day,3,30,tzinfo=Pacific)
  
  if Pacific.dst(dAfter) == Pacific.dst(dBefore):
    return (False, None)
  else:
    if dAfter.utcoffset() == 7:
      isSpring = True
    else:
      isSpring = False
    return (True, isSpring)

def processNormalDay(day, times):
  i = 0
  d = date(2010,1,2)
  d1 = datetime.combine(d,times[0])
  d2 = datetime.combine(d,times[1])
  diff = d2 - d1

  measurements = list()
  while i < len(times):
    start = datetime.combine(day.day, times[i])
    energy = day.readings[i]
    if i != len(times) - 1:
      end = datetime.combine(day.day, times[i+1])
    else:
      end = datetime.combine(day.day, times[i])
      end += diff
    i += 1
    measurements.append(DurationalMeasurement(start,end,energy))

  return measurements

if __name__ == '__main__':
  (args, options) = ParseArguments()
  token = args[0]
  variable = args[1]
  filename = args[2]

  with open(filename, 'r') as f:
    csvReader = csv.reader(f,delimiter=',',quotechar='"')
    
    times = list()
    days = list()
    for row in csvReader:
      if len(row) > 0:
        if row[0].count('/') == 0:
          if not row[0].startswith('kWh'):
            parseHeader(row)
          else:
            times = parseTimes(row)
        else:
          if not row[0].startswith('Missing data'):
                days.append(parseDay(row))
          else:
            handleMissingData(row,days)

  if len(times) <= 0:
    print 'Error: Read input file, but never read the time header.'
    exit(1)
  if len(days) <= 0:
    print 'Error: Read input file, but never parsed any electricity usage data.'
    exit(1)

  readings = list()
  for day in days:
    if len(day.readings) == len(times):
      (isDST, isSpring) = isDSTBoundary(day.day)
      if isDST:
        readings.append(processDSTDay(day, times, isSpring))
        print 'Warning: Handling DST transition. This code is fragile.'
      else:
        for measurement in processNormalDay(day,times):
          readings.append(measurement)
    else:
      print "Warning: There are %d energy readings but %d associated timeslots for day %s." % (len(readings),len(times),day.day.isoformat())
      print "\tPlease upload your file to the wiki, or provide a patch to handle your input."
      
  print "Info: Processed %d durational readings." % len(readings)

  log = google_meter.Log(1)

  service = google_meter.Service(token, options.service, log=log)
  
  service = google_meter.BatchAdapter(service)

  meter = google_meter.Meter(
      service, variable, options.uncertainty * units.KILOWATT_HOUR,
      options.time_uncertainty, True)

  for reading in readings:
    start = rfc3339.FromTimestamp(reading.dStart.isoformat())
    end = rfc3339.FromTimestamp(reading.dEnd.isoformat())
    meter.PostDur(start,end,reading.energy * units.KILOWATT_HOUR,reading.uncertainty * units.KILOWATT_HOUR)
  service.Flush()

