#!/usr/bin/python2.6 
# pgeToGoogleFormat.py
# 	Reads and transmit PG&E SmartMeter CSV data to Google PowerMeter.
# 	http://gitorious.org/pge-to-google-powermeter/
#
#   Copyright (C) 2010  Andrew Potter
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
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
# The variable is then                  "/user/1234/5678/variable/abcde.d1"
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
# Features: Directly uploads to Google PowerMeter. 
#           Handles Daylight Savings transitions correctly as of 2010. 
#               
#
# Bugs / Assumptions:
#
# A1) Assumes times given are all in Pacific time. Which is how the
# files I've downloaded from PG&E are specified. Unfortunately the
# header data does not specify the time zone, so we have to make this
# assumption.
# 
# A2) Expects only hourly data. If PG&E ever offers higher resolution,
# two things must happen:
#    1) in parseHeaders(), don't abort if the title isn't 'Hourly Usage'
#    2) processDSTDay() has to be modified to handle whatever the new format is.
#
#    Basically, other than handling DST transitions everything should
#    be ready to go as long as the time header is the same.
#
# B2) I do a lot of Error/Warning/Info printfs, but I should probably use the
# Log class. 
#

import os
import sys
import csv
import time as sleeptime
from datetime import tzinfo, timedelta, datetime, date, time
from optparse import OptionParser
import google_meter
import units
import rfc3339
import ConfigParser as cp

programVersion = '0.9'
programName = 'pge2google'
config_filename = 'config'

def parseArguments():
  op = OptionParser('%prog [--token <token>] [--variable <variable>] Filename.csv [File2.csv [...]]\n\n' + '''
arguments:
  Filename.csv        The Hourly usage CSV datafile from PG&E (required)''', version="%s %s" % (programName, programVersion))
  op.add_option('', '--token', metavar='<token>',
                help='Google PowerMeter OAUTH Token'
                     ' (default: None)')
  op.add_option('', '--variable', metavar='<variable>',
                help='Google PowerMeter Variable'
                     ' (default: None)')
  op.add_option('', '--service', metavar='<URI>',
                help='URI prefix of the GData service to contact '
                     '(default: https://www.google.com/powermeter/feeds)')
  op.add_option('-f','--configFile', metavar='<configFile>', help="Path and filename of configuration file (default: ~/.local/%s/config)" % programName)
  op.add_option('-d','--debug', dest="isDebug", action="store_true", help="Disable upload to Google (default: false)", default=False)

  op.set_defaults(service='https://www.google.com/powermeter/feeds',
                  unit='kW h', uncertainty=0.001, time_uncertainty=1)

  # Parse and validate the command-line options.
  options, args = op.parse_args()

  # Check for config file, setup default otherwise
  if options.configFile == None:
      home = os.getenv('HOME')
      config_home = os.getenv('XDG_CONFIG_HOME',"%s/.local/" % home)
      config_dir = "%s%s" % (config_home,programName)
      filepath = "%s/%s" % (config_dir, config_filename)
      if os.path.exists(config_home):
        if os.path.exists(config_dir):
          if os.path.exists(filepath):
            options.configFile = filepath
  else:
    if not os.path.exists(options.configFile):
      if os.path.exists(os.getcwd() + options.configFile):
        options.configFile = os.getcwd() + options.configFile
      else:
        sys.stderr.write("Error: Can not find config file '%s'\n" % options.configFile)
        exit(2)
  
  if options.token == None:
    if checkConfigfile(options.configFile,'token'):
      options.token = getConfigfile(options.configFile,'token')
    else:
      sys.stderr.write('Error: Missing Google Power Meter OAuth token. \nToken must be supplied via --token or in the config file.\n')
      op.exit(2, op.format_help())
  if options.variable == None:
    if checkConfigfile(options.configFile,'variable'):
      options.variable = getConfigfile(options.configFile,'variable')
    else:
      sys.stderr.write('Error: Missing Google Power Meter variable.\nVariable must be supplied via --variable or in the config file.\n')
      op.exit(2,op.format_help())

  if len(args) < 1:
    sys.stderr.write('Error: No input file specified.\n')
    op.exit(2, op.format_help())

  return (args, options)

def checkConfigfile(filename,var):
  with open(filename) as f:
    parser = cp.SafeConfigParser()
    try:
      parser.readfp(f)
      hasVar = parser.has_option('main',var)
      return hasVar
    except cp.MissingSectionHeaderError:
      sys.stderr.write("Error: Config file seems to be invalid (Missing Section 'main')\n")
      exit(1)
  return False

def getConfigfile(filename,var):
  with open(filename) as f:
    parser = cp.SafeConfigParser()
    parser.readfp(f)
    return parser.get('main',var)
  return None


ZERO = timedelta(0)
HOUR = timedelta(hours=1)
DAY  = timedelta(days=1)

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

class ZuluTimeZone(tzinfo):
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
        return ZERO

    def dst(self, dt):
      return ZERO

Zulu     = ZuluTimeZone(0, "Zulu", "Z", "Z")
Eastern  = USTimeZone(-5, "Eastern",  "EST", "EDT")
Central  = USTimeZone(-6, "Central",  "CST", "CDT")
Mountain = USTimeZone(-7, "Mountain", "MST", "MDT")
Pacific  = USTimeZone(-8, "Pacific",  "PST", "PDT")

def parseHeader(row, headers):
  if (len(row) == 2):
    headers[row[0]] = row[1]

  if row[0] == 'Title':
    if row[1] != 'Hourly Usage':
      print 'Error: Input file is not "Hourly Usage"-type!'
      exit(1)
  elif row[0] == 'Primary Data Unit':
    if row[1] != 'kWh' and row[1] != 'k Wh':
      print 'Error: Primary Data Unit is ' + row[1]
      print 'Error: The only supported unit is kWh!'
      exit(1)

def handleMissingData(row, days):
  """Removes the day element from days that corresponds to the missing data."""
  # Do nothing. PG&E will report that data is missing for a day even if it is missing
  # only 1 hour of data -- such as on 3/14 (DST Spring). 
  #
  # Rather than taking PG&E's hint, we should handle this in parseToReadings

def parseTimes(times):
  # Parse the times
  convTimes = list()
  times.pop(0) # Get rid of the header
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
  return convTimes

def parseDay(row):
  # Grab the date from the first column
  # and the measurements from the rest
  datestr = row.pop(0)

  dashcount = 0
  readings = list()
  for reading in row:
    if reading != '-':
      readings.append(float(reading))
    else:
      # TODO: We should simply ignore this data point.
      #       This requires changing the dataflow, so that 'times' is passed in here
      #       and the DurationalMeasurements are produced here rather than later.
      readings.append(float(0))
      dashcount += 1

  if dashcount == 24:
    readings = list()
    print "Warning: Input file has no valid readings for %s." % datestr

  # Parse the date
  # Sometimes PG&E has double quotes, sometimes not.
  # TODO: This may be redundant now that I use csvreader
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
    self.uncertainty = DurationalMeasurement.defaultUncertainty

  def setUncertainty(self, uncertainty):
    self.uncertainty = uncertainty

def isDSTBoundary(day):
  dBefore = datetime(day.year,day.month,day.day,0,30,tzinfo=Pacific)
  dAfter = datetime(day.year,day.month,day.day,3,30,tzinfo=Pacific)
  
  if Pacific.dst(dAfter) == Pacific.dst(dBefore):
    return (False, None)
  else:
    if dAfter.utcoffset() == timedelta(days=-1,hours=17):
      isSpring = True
    else:
      isSpring = False
    return (True, isSpring)

def processNormalDay(day, times):
  measurements = list()
  i = 0
  for i in range(len(times)):
    start = datetime.combine(day.day, times[i])
    energy = day.readings[i]
    if i != len(times) - 1:
      end = datetime.combine(day.day, times[i+1])
    else:
      end = datetime.combine(day.day, times[0]) + DAY
    measurements.append(DurationalMeasurement(start,end,energy))
  return measurements

def processDSTDay(day, times, isSpring): 
  measurements = list()

  # TODO: This has to change if postings not hourly
  if len(times) != 24:
    sys.stderr.write("Error: DST transition on %s, but data is not strictly hourly.\n" % day)
    sys.stderr.write('\tPlease make a patch and/or alert the project at:\n\thttp://gitorious.org/pge-to-google-powermeter/pages/Home\n')
    sys.stderr.write('\tProcessing of other files will continue if possible.')
    print len(times)
    return measurements
  # End TODO

  if (isSpring):
    # We are springing ahead, 2 AM becomes 3 AM.
    # In PG&E's file, they have a '-' for 2 AM
    for i in range(len(times)):
      start = datetime.combine(day.day, times[i])
      energy = day.readings[i]
      if i == 1:
        end = datetime.combine(day.day, times[i+2])
      elif i == 2: #TODO: This has to change if postings not hourly
        # Do nothing
        print 'Info: Springing ahead 1 hour.'
        continue
      elif i == (len(times) - 1):
        end = datetime.combine(day.day, times[0]) + DAY
      else:
        end = datetime.combine(day.day, times[i+1])
      measurements.append(DurationalMeasurement(start,end,energy))
  else:
    # We are Falling behind. 3 am becomes 2 AM.
    # In PG&E's file, the 1 AM-2 AM slot has 2 hours worth of data
    # So it is really 1 AM - 3 AM, in a DSTless world.
    for i in range(len(times)):
      start = datetime.combine(day.day, times[i])
      energy = day.readings[i]
      if i == 0:
        end = datetime.combine(day.day, time(8,0,0,tzinfo=Zulu))
      elif i == 1: #TODO: This has to change if postings not hourly
        print 'Info: Falling behind 1 hour.'
        start = datetime.combine(day.day, time(8,0,0,tzinfo=Zulu))
        end = datetime.combine(day.day, time(10,0,0,tzinfo=Zulu))
      elif i == (len(times)-1):
        end = datetime.combine(day.day, times[0]) + DAY
      else:
        end = datetime.combine(day.day, times[i+1])
      measurements.append(DurationalMeasurement(start,end,energy))                           
  return measurements

def readfile(filename):
  with open(filename, 'r') as f:
    csvReader = csv.reader(f,delimiter=',',quotechar='"')
    times = list()
    days = list()
    headers = dict()

    for row in csvReader:
      if len(row) > 0:
        if row[0].count('/') == 0: # Test for date field, e.g. 3/14/2010
          if not row[0].startswith('kWh'): # Time header's first field
            parseHeader(row, headers)
          else:
            times = parseTimes(row)
        else:
          # Following two if statements weed out info from Daily reports.
          if row[0].startswith('Cost') or row[0].startswith('per kWh'):
            continue
          if len(row) > 1:
            if row[1].count('$') > 0:
              continue
          if not row[0].startswith('Missing data'):
            days.append(parseDay(row))
          else:
            handleMissingData(row, days)
    return (times, days)
            
def parseToReadings(times, days):
  readings = list()
  for day in days:
    if len(day.readings) == len(times):
      (isDST, isSpring) = isDSTBoundary(day.day)
      if isDST:
        for measurement in processDSTDay(day, times, isSpring):
          readings.append(measurement)
      else:
        for measurement in processNormalDay(day,times):
          readings.append(measurement)
    elif len(day.readings) > 0:
      print "Warning: There are %d energy readings but %d associated timeslots for day %s." % (len(readings),len(times),day.day.isoformat())
      print '\tPlease upload your data file to the wiki (strip sensitive info!), and/or provide a patch to handle your input.'
  return readings

if __name__ == '__main__':
  (filenames, options) = parseArguments()

  token = options.token
  variable = options.variable
  readings = list()

  for filename in filenames:
    (times, days) = readfile(filename)

    if len(times) <= 0:
      sys.stderr.write('Error: Read input file, but never read the time header.\n')
      sys.stderr.write("Ignoring file '%s'\n" % filename)
      continue
    if len(days) <= 0:
      sys.stderr.write('Error: Read input file, but never parsed any electricity usage data.\n')
      sys.stderr.write("Ignoring file '%s'\n" % filename)
      continue

    readings.extend(parseToReadings(times, days))
  
      
  print "Info: Processed %d durational readings. Now attempting to upload to Google." % len(readings)

  log = google_meter.Log(1)
  service = google_meter.Service(token, options.service, log=log)
  service = google_meter.BatchAdapter(service)
  meter = google_meter.Meter(
      service, variable, options.uncertainty * units.KILOWATT_HOUR,
      options.time_uncertainty, True)

  for i in range(len(readings)/1000 + 1):
    if len(readings) > 1000:
      for j in range(1000):
        reading = readings.pop()
        start = rfc3339.FromTimestamp(reading.dStart.isoformat())
        end = rfc3339.FromTimestamp(reading.dEnd.isoformat())
        meter.PostDur(start,end,reading.energy * units.KILOWATT_HOUR,reading.uncertainty * units.KILOWATT_HOUR)
    else:
      for reading in readings:
        start = rfc3339.FromTimestamp(reading.dStart.isoformat())
        end = rfc3339.FromTimestamp(reading.dEnd.isoformat())
        meter.PostDur(start,end,reading.energy * units.KILOWATT_HOUR,reading.uncertainty * units.KILOWATT_HOUR)
      if not options.isDebug:
        service.Flush()
      break
    if not options.isDebug:
      service.Flush()
    print "There remains %d measurements to upload." % len(readings)
    for k in range(10):
      print "Sleeping for %d minutes." % (10-k)
      sleeptime.sleep(60)



