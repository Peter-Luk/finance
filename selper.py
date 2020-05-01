# from selenium import webdriver
from pref import Path, os, driver

def driver_path(browser, sub_path=['browser','driver']):

    _ = [str(Path.home())]
    _.extend(sub_path)
    _.append(driver[browser.capitalize()])
    return os.sep.join(_)

# Start Chrome
# cb = webdriver.Chrome(executable_path=driver_path('Chrome'))
# Start Firefox
# fb = webdriver.Firefox(executable_path=driver_path('Firefox'))
# Start Internet Explorer
# ib = webdriver.Ie(executable_path=driver_path('Ie'))
