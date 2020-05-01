# from selenium import webdriver
from pref import Path, os, driver

def driver_path(browser):
    _ = [str(Path.home())]
    __ = driver[browser.capitalize()]
    _.extend(__.path)
    _.append(__.name)
    return os.sep.join(_)

# Start Chrome
# cb = webdriver.Chrome(executable_path=driver_path('Chrome'))
# Start Firefox
# fb = webdriver.Firefox(executable_path=driver_path('Firefox'))
# Start Internet Explorer
# ib = webdriver.Ie(executable_path=driver_path('Ie'))
