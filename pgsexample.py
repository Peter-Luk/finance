import time
import gdata.spreadsheet.service

email = 'peterluk.phillip@gmail.com'
password = '80192641'
weight = '180'
# Find this value in the url with 'key=XXX' and copy XXX below
spreadsheet_key = '1JQuU8YnwxuCV0EN84TkBtcxzWA_wWL9Phl55eBqwRBU'
# All spreadsheets have worksheets. I think worksheet #1 by default always
# has a value of 'od6'
worksheet_id = 'od6'

spr_client = gdata.spreadsheet.service.SpreadsheetsService()
spr_client.email = email
spr_client.password = password
spr_client.source = 'Example Spreadsheet Writing Application'
spr_client.ProgrammaticLogin()

# Prepare the dictionary to write
#dict = {}
#dict['date'] = time.strftime('%m/%d/%Y')
#dict['time'] = time.strftime('%H:%M:%S')
#dict['weight'] = weight
#print dict

#entry = spr_client.InsertRow(dict, spreadsheet_key, worksheet_id)
#if isinstance(entry, gdata.spreadsheet.SpreadsheetsList):
#  print "Insert row succeeded."
#else:
#  print "Insert row failed."
