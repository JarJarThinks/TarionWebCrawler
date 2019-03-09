import requests
from bs4 import BeautifulSoup
import csv
import time
import wx
from threading import *

EVT_UPDATE_ID = wx.ID_ANY
def EVT_UPDATE(win, func):
	win.Connect(-1,-1, EVT_UPDATE_ID,func)

class UpdateEvent(wx.PyEvent):
	def __init__(self, data):
		wx.PyEvent.__init__(self)
		self.SetEventType(EVT_UPDATE_ID)
		self.data = data
		
class ScrapperThread(Thread):
	def __init__(self, notify_window,arg):
		Thread.__init__(self)
		self.setDaemon(1)
		self._notify_window=notify_window
		self._arg = arg
		self._want_abort = False
		self.start()
		
	def run(self):
		ROW_HEADERS = [
		'Building/Vendor Name',
		'Umbrella Group',
		'Location (city)',
		'Registration #',
		'Registration Status',
		'Designation Type',
		'Sales & Marketing',
		'Address',
		'Tel',
		'Fax',
		'Website',
		'Email',
		'Directors',
		'Officers',
		'Locations',
		'Possessions',
		'Enrolments',
		'Chargeable Conciliations',
		'Homes With Claims',
		'Dollars Paid in Claims'
		]

		for year in range (2008,2019):
			ROW_HEADERS.append('Freehold Possessions ' + str(year))
			ROW_HEADERS.append('Condo Unit Possessions ' + str(year))
			ROW_HEADERS.append('Total Possessions ' + str(year))
			ROW_HEADERS.append('Chargeable Conciliations ' + str(year))
			ROW_HEADERS.append('Homes with Claims (Excluding Major Structural Defects) ' + str(year))
			ROW_HEADERS.append('Dollars Paid in Claims (Excluding Major Structural Defects) ' + str(year))
			ROW_HEADERS.append('Homes with Major Structural Defect Claims ' + str(year))
			ROW_HEADERS.append('Dollars Paid in Major Structural Defect Claims ' + str(year))
			ROW_HEADERS.append('Total Dollars Paid in Claims ' + str(year))
			
		ROW_HEADERS.append('Freehold Possessions Total')
		ROW_HEADERS.append('Condo Unit Possessions Total')
		ROW_HEADERS.append('Total Possessions Total')
		ROW_HEADERS.append('Chargeable Conciliations Total')
		ROW_HEADERS.append('Homes with Claims (Excluding Major Structural Defects) Total')
		ROW_HEADERS.append('Dollars Paid in Claims (Excluding Major Structural Defects) Total')
		ROW_HEADERS.append('Homes with Major Structural Defect Claims Total')
		ROW_HEADERS.append('Dollars Paid in Major Structural Defect Claims Total')
		ROW_HEADERS.append('Total Dollars Paid in Claims Total')

		start_page_num = 0

		headers = {
			'User-Agent': 'Jarod Lee',
			'From': 'jlee431@uottawa.com'
		}
		file_name = self._arg[0]

		if (self._arg[1] == 0):
			file_mode = 'w'
		else:
			file_mode = 'a'

		f = csv.writer(open(file_name+'.csv', file_mode))

		if (file_mode == 'w'):
			f.writerow(ROW_HEADERS)

		page_url = self._arg[2]
		if(page_url.find('www.tarion.com/ontariobuilderdirectory/search') == -1):
			wx.PostEvent(self._notify_window, UpdateEvent('URL is not valid a Tarion search, please check the URL\n'))
			wx.PostEvent(self._notify_window, UpdateEvent(True))
			return
		 #try to load the search url
		try:
			page = requests.get(page_url, headers = headers)
		except Exception as ex:
			template = "\nAn exception of type {0} occurred. Stopping crawler. Arguments:\n{1!r}"
			message = template.format(type(ex).__name__, ex.args)
			wx.PostEvent(self._notify_window,UpdateEvent(message))
			wx.PostEvent(self._notify_window,UpdateEvent(True))
			
		if (page.status_code != 200):
			wx.PostEvent(self._notify_window, UpdateEvent('Failed to load page. Check URL and internet connection.\n'))
			wx.PostEvent(self._notify_window, UpdateEvent(True))
			return
		#turn the requests page into a BeautifulSoup for easier handling
		
		soup = BeautifulSoup(page.text, 'html.parser')
		table = soup.find('tbody') #get the table
		if (table is None):
			wx.PostEvent(self._notify_window, UpdateEvent('Unable to find search results table. Check the URL\n'))
			wx.PostEvent(self._notify_window, UpdateEvent(True))
			return

		last_page = soup.find(class_='pager__item pager__item--last')
		last_page_url = last_page.find('a').get('href')
		searchURL = last_page_url[:last_page_url.find("&page=")+6]

		last_page_num = int(last_page_url[last_page_url.find("&page=")+6:])

		start_page_num = self._arg[3]
		end_page_num = self._arg[4]

		if (not start_page_num.isnumeric()):
			wx.PostEvent(self._notify_window, UpdateEvent('Invalid start page, input is not a number\n'))
			wx.PostEvent(self._notify_window, UpdateEvent(True))
			return
		elif (isinstance(start_page_num, int)):
			wx.PostEvent(self._notify_window, UpdateEvent('Invalid start page, input is not a whole number\n'))
			wx.PostEvent(self._notify_window, UpdateEvent(True))
			return
		elif (int(start_page_num) < 1):
			wx.PostEvent(self._notify_window, UpdateEvent('Invalid start page, input is a negative number or is zero.\n'))
			wx.PostEvent(self._notify_window, UpdateEvent(True))
			return
		else:
			start_page_num = int(start_page_num) - 1
			if (last_page_num < start_page_num):
				wx.PostEvent(self._notify_window, UpdateEvent("Last page of search lower than expected start page, just scrapping the last page.\n\n"))
				start_page_num = last_page_num
			
				
		if (not end_page_num.isnumeric()):
			wx.PostEvent(self._notify_window, UpdateEvent('Invalid end page, input is not a number\n'))
			
			wx.PostEvent(self._notify_window, UpdateEvent(True))
			return
		elif (isinstance(end_page_num, int)):
			wx.PostEvent(self._notify_window, UpdateEvent('Invalid end page, input is not a whole number\n'))
			
			wx.PostEvent(self._notify_window, UpdateEvent(True))
			return
		elif (int(end_page_num) < 1):
			wx.PostEvent(self._notify_window, UpdateEvent('Invalid end page, input is a negative number or is zero.\n'))
			
			wx.PostEvent(self._notify_window, UpdateEvent(True))
			return
		else:
			end_page_num = int(end_page_num)
			if (last_page_num < end_page_num):
				wx.PostEvent(self._notify_window,UpdateEvent("Specified end page higher than actual search pages, just scrapping upto the last page.\n\n"))
				end_page_num = last_page_num
			if (end_page_num <= start_page_num):
				wx.PostEvent(self._notify_window,UpdateEvent("Specified end page less than start page, Reading one page of data.\n\n"))
				end_page_num = start_page_num+1

				
		start_row = self._arg[5]
		start_row = int(start_row) - 1
		
		delay_time = int(self._arg[6])

		pages = [] #pages contains the strings of the URLs for all the pages in the search
		for i in range(1, last_page_num + 1): #the pages list is one behind since we're not counting page 0, ie page[22] refers to ...&page=23
			url = page_url[:53] + searchURL + str(i)
			pages.append(url)

		#START OF PAGE SCRAPPING
		wx.PostEvent(self._notify_window,UpdateEvent("Starting data scrapping from page " +str(start_page_num+1) + " to page " +str(end_page_num) +"\n"))
		for i in range(start_page_num,end_page_num):
			if self._want_abort:
				wx.PostEvent(self._notify_window, UpdateEvent(True))
				return
			if (i>0): #if not parsing the loaded page, load the new page
				start_row = 0
				loaded = False
				while (not loaded):
					try:
						page = requests.get(pages[i-1], headers = headers)
					except Exception as ex:
						template = "\nAn exception of type {0} occurred. Stopping crawler. Arguments:\n{1!r}"
						message = template.format(type(ex).__name__, ex.args)
						wx.PostEvent(self._notify_window,UpdateEvent(message))
						wx.PostEvent(self._notify_window,UpdateEvent(True))
						
					if (page.status_code != 200):
						wx.PostEvent(self._notify_window, UpdateEvent('Failed to load page. Retrying in 5 seconds. Check internet connection.\n'))
						time.sleep(5)
					else:
						soup = BeautifulSoup(page.text, 'html.parser')
						table = soup.find('tbody') #get the table
						if (table is None): #if it failed to load
							wx.PostEvent(self._notify_window,UpdateEvent("Page didn't load, retrying in 5 seconds\n"))
							time.sleep(5)
						else:
							loaded = True
			table_rows = table.find_all('tr')
			wx.GetApp().Yield()

			if self._want_abort:
					wx.PostEvent(self._notify_window, UpdateEvent(True))
					return
			
			if (start_row >= len(table_rows)):
				start_row = len(table_rows) - 1
			for x in range(start_row, len(table_rows)): #scrapping table rows
				items = table_rows[x].find_all('td')
				out = []
				loaded = False
				#pulling the information from the profile page
				while (not loaded):
					profile_url = page_url[:22] + items[0].find('a').get('href')
					try:
						profile_page = requests.get(profile_url, headers = headers)
					except Exception as ex:
						template = "\nAn exception of type {0} occurred. Stopping crawler. Arguments:\n{1!r}"
						message = template.format(type(ex).__name__, ex.args)
						wx.PostEvent(self._notify_window,UpdateEvent(message))
						wx.PostEvent(self._notify_window,UpdateEvent(True))
					
					if (profile_page.status_code != 200):
						wx.PostEvent(self._notify_window, UpdateEvent('Failed to load page. Retrying in 10 seconds. Check internet connection.\n'))
						time.sleep(10)
					else:
						loaded = True
					profile_soup = BeautifulSoup(profile_page.text, 'html.parser')
					company_info = profile_soup.find(class_="block block-builder-directory block-company-information-block clearfix")

						
				profile_tables = profile_soup.find_all('tbody') #get the "At a Glance" table and the "Possessions Information" Table
				glance_rows = profile_tables[0].find_all('tr') #split the rows of the "At a Glance" table
				possessions_rows = profile_tables[1].find_all('tr') #split the rows of the "Possessions" table

				if self._want_abort:
					wx.PostEvent(self._notify_window, UpdateEvent(True))
					return
					
				info = company_info.find_all('li')
				
				top_left_block = profile_soup.find(class_="block-style-1 full-border-radius views-row")
				top_left_items = top_left_block.find_all('li')

				out.append(items[0].contents[0].contents[0].strip()) #name[7]
				if (len(items[1].contents) == 2):
					out.append(items[1].contents[0].contents[0].strip()) #Umbrella
				else:
					out.append(items[1].contents[0].strip()) #Umbrella

				out.append(items[2].contents[0].strip()) #Location
				out.append(items[3].contents[0].contents[0].strip()) #Reg Number
				out.append(items[4].contents[0].strip()) #Registration Status
				out.append(top_left_items[1].contents[1].strip()) #designation
				for u in range(10):
					if (u != 6):
						if (len(info[u].contents) == 3):
							out.append(info[u].contents[2].strip())
						else:
							out.append(info[u].contents[3].contents[0].strip())
				for g_row in glance_rows: #scrapping table rows
					out.append(g_row.contents[3].contents[0].strip())
				
				for p_row in possessions_rows:
					for a in range(3,len(p_row),2):
						out.append(p_row.contents[a].contents[0])
				
				f.writerow(out)

				wx.PostEvent(self._notify_window,UpdateEvent("Page "+ str(i+1) + " Row "+ str(x+1) + ": "+ out[0] + " DONE\n"))
				if self._want_abort:
					wx.PostEvent(self._notify_window, UpdateEvent(True))
					return
				time.sleep(delay_time)
			
		wx.PostEvent(self._notify_window,UpdateEvent("\nData Scrape from page " +str(start_page_num+1) + " to page " +str(end_page_num) +" finished\n"))
		wx.PostEvent(self._notify_window, UpdateEvent(True))
	def abort(self):
		self._want_abort = True

class Frame(wx.Frame):
	def __init__(self, parent, title):
		wx.Frame.__init__(self, parent, title=title, size=(500, 612))
		self.panel = wx.Panel(self)
		self.Bind(wx.EVT_CLOSE, self.OnCloseWindow)
		self.Startbtn = wx.Button(self.panel, -1, "Start")
		self.Bind(wx.EVT_BUTTON, self.OnStart, self.Startbtn)
		self.Stopbtn = wx.Button(self.panel, -1, "Stop")
		self.Bind(wx.EVT_BUTTON, self.OnStop, self.Stopbtn)
		
		self.txt1 = wx.StaticText(self.panel, -1, 'Search URL:')
		self.URLtxt = wx.TextCtrl(self.panel, -1, size=(500,-1))
		self.URLtxt.SetValue('https://www.tarion.com/ontariobuilderdirectory/search?status=registered')
		
		self.txt2 = wx.StaticText(self.panel, -1,'File Name:')
		self.Filetxt = wx.TextCtrl(self.panel, -1, size=(350,-1))
		self.Filetxt.SetValue('tarion')
		
		self.Starttxt = wx.TextCtrl(self.panel,-1, size=(300,-1))
		self.Starttxt.SetValue('1')
		self.Endtxt = wx.TextCtrl(self.panel, -1,size=(300,-1))
		self.Endtxt.SetValue('300')
		self.RowSlider = wx.Slider(self.panel, -1, minValue=1, maxValue = 25, size = (400,-1))
		self.RowSlider.SetValue(1)
		self.Rowtxt = wx.TextCtrl(self.panel, -1, size=(50,-1))
		self.Rowtxt.SetValue('1')
		self.Rowtxt.SetEditable(False)
		self.Bind(wx.EVT_SCROLL, self.RowSlideUpdate, self.RowSlider)
		
		self.TimeSlider = wx.Slider(self.panel, -1, minValue=3, maxValue = 15, size = (400,-1))
		self.TimeSlider.SetValue(5)
		self.Timetxt = wx.TextCtrl(self.panel, -1, size=(50,-1))
		self.Timetxt.SetValue('5 sec')
		self.Timetxt.SetEditable(False)
		self.Bind(wx.EVT_SCROLL, self.TimeSlideUpdate, self.TimeSlider)
		
		
		modeList =["Overwrite", "Append"]
		self.rbox = wx.RadioBox(self.panel,label = 'File Write Mode', pos = (80,10), choices = modeList , majorDimension = 1, style = wx.RA_SPECIFY_ROWS)
		
		self.Statustxt = wx.TextCtrl(self.panel, -1, size=(500,300), style=wx.TE_MULTILINE|wx.TE_READONLY)
		self.Statustxt.SetEditable(False)

		self.txt3 = wx.StaticText(self.panel, -1,'Start Page:')
		self.txt4 = wx.StaticText(self.panel, -1,'End Page:')
		self.txt5 = wx.StaticText(self.panel, -1,'Start Row:')
		self.txt6 = wx.StaticText(self.panel, -1,' .csv')
		self.txt7 = wx.StaticText(self.panel, -1,'Time between page loads:')

		sizer = wx.BoxSizer(wx.VERTICAL)
		rowSizer = wx.BoxSizer(wx.HORIZONTAL)
		timeSizer = wx.BoxSizer(wx.HORIZONTAL)
		startStopSizer = wx.BoxSizer(wx.HORIZONTAL)
		fileSizer = wx.BoxSizer(wx.HORIZONTAL)
		sizer.Add(self.txt1)
		sizer.Add(self.URLtxt)
		sizer.Add(self.txt2)
		
		fileSizer.Add(self.Filetxt)
		fileSizer.Add(self.txt6)
		sizer.Add(fileSizer)
		
		sizer.Add(self.rbox)
		sizer.Add(self.txt3)
		sizer.Add(self.Starttxt)
		sizer.Add(self.txt4)
		sizer.Add(self.Endtxt)
		sizer.Add(self.txt5)

		rowSizer.Add(self.Rowtxt)
		rowSizer.Add(self.RowSlider)
		sizer.Add(rowSizer)
		
		sizer.Add(self.txt7)
		timeSizer.Add(self.Timetxt)
		timeSizer.Add(self.TimeSlider)
		sizer.Add(timeSizer)
		
		startStopSizer.Add(self.Startbtn)
		startStopSizer.Add(self.Stopbtn)
		sizer.Add(startStopSizer)
		sizer.Add(self.Statustxt)
		
		EVT_UPDATE(self,self.Update)
		self.worker = None
	
		self.panel.SetSizer(sizer)
		self.Show()
		
	def OnStart(self, event):
		if not self.worker:
			arg =[self.Filetxt.GetValue(), self.rbox.GetSelection(), self.URLtxt.GetValue(), self.Starttxt.GetValue(),self.Endtxt.GetValue(),self.RowSlider.GetValue(), self.TimeSlider.GetValue()]
			if arg[1] == 0:
				confirm = wx.MessageDialog(self.panel, 'You are about to overwrite ' + arg[0]+'.csv.\nAre you sure you want to do this?' ,
					caption="Overwrite Warning", style=wx.YES_NO|wx.CENTRE)
				if (confirm.ShowModal() != wx.ID_YES):
					return
				
			self.worker = ScrapperThread(self, arg)
			
	def OnStop(self,event):
		if self.worker:
			self.Statustxt.AppendText('\nStopping the Crawler\n')
			self.worker.abort()
		
	def RowSlideUpdate(self,e):
		self.Rowtxt.SetValue(str(self.RowSlider.GetValue()))
	def TimeSlideUpdate(self,e):
		self.Timetxt.SetValue(str(self.TimeSlider.GetValue()) + " sec")
		
	def Update(self, event):
		if event.data == True:
			self.worker = None
		else:
			self.Statustxt.AppendText(event.data)

	def OnCloseWindow(self, e):
		self.worker = None
		self.Destroy()
app = wx.App(redirect=True)
top = Frame(None,"Tarion Search Engine Crawler")
top.Show()
app.MainLoop()