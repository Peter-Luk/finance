from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time

from utilities import driver_path, mtf, linesep
from pt_2 import PI
_ = 'Firefox'
i2 = PI(mtf('mhi'))

# Replace below path with the absolute path 
# to chromedriver in your computer 
driver = eval(f"webdriver.{_}(executable_path=driver_path('{_}'))")
# driver = webdriver.Chrome('/home/saket/Downloads/chromedriver') 

driver.get("https://web.whatsapp.com/")
wait = WebDriverWait(driver, 600)

# Replace 'Friend's Name' with the name of your friend 
# or the name of a group 
target = '"凌月明Milly"'

# Replace the below string with your own message 
# string = "Message sent using Python!!!"
string = linesep.join([i2.estimate(pivot), f"range between {max(i2.estimate(pivot)['Daily']['upper'])} and {min(i2.estimate(pivot)['Daily']['lower'])}"])

x_arg = '//span[contains(@title,' + target + ')]'
group_title = wait.until(EC.presence_of_element_located((By.XPATH, x_arg)))
group_title.click()
inp_xpath = '//div[@class="input"][@dir="auto"][@data-tab="1"]'
input_box = wait.until(EC.presence_of_element_located((By.XPATH, inp_xpath)))
for i in range(100):
	input_box.send_keys(string + Keys.ENTER)
	time.sleep(1)
