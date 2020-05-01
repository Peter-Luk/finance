from selenium import webdriver
from pathlib import Path, os

def driver_path(browser, sub_path=['browser','driver']):
    browser_type = {'Chrome':'chromedriver.exe', 'Firefox':'geckodriver.exe', 'Ie':'IEDriverServer.exe'}
    
    _ = [str(Path.home())]
    _.extend(sub_path)
    _.append(browser_type[browser.capitalize()])
    return os.sep.join(_)

# Start Chrome
cb = webdriver.Chrome(executable_path=driver_path('Chrome'))
# Start Firefox
fb = webdriver.Firefox(executable_path=driver_path('Firefox'))
# Start Internet Explorer
ib = webdriver.Ie(executable_path=driver_path('Ie'))
